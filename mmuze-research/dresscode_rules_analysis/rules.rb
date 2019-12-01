require 'mongo'
require 'set'
require 'csv'

class Rules



  GENDERS = ["Female","Male","Unisex"]
  COLOR_GROUPS = ["metallic" ,"pale" ,"neon" ,"bright" ,"washed" ,"pattern" ,"dark" ,"gradient" ,"neutral color" ,"warm color" ,"cool color"]
  ITEMS_DIRECTORY = "data_items"
  DOCS_DIRECTORY  = "data_docs"

  def initialize retailer_products = "retailer_products"
    Mongo::Logger.logger.level = ::Logger::FATAL
    @mongo = Mongo::Client.new([ '127.0.0.1:27017' ], :database => 'marketpulzz')
    @retailer_products = @mongo[retailer_products]
    @color_aspects = @mongo[:color_aspects]
    @aspect_connections  = @mongo[:aspect_connections]
    @product_types = @mongo[:product_types]
    @product_types_uk = @mongo[:product_types_uk]
    @dress_codes_tags = @mongo[:dress_code_tags]
    @colors_hash = @color_aspects.aggregate([{"$project"=>{"name"=>1,"_id"=>0}},
                                             {"$group"=>{"_id"=>nil,"color_names"=>{"$push"=>"$name"} }},
                                             {"$project"=>{"color_names"=>1,"_id"=>0}}]).first
    @color_names = @colors_hash["color_names"].map{|c| c.downcase}
    @occasions_mapped_products=[]

    a = @mongo[:occasions_products].aggregate([{"$match"=>{"occasions_products.dress_code"=>"festive"}},{"$project" =>{ "_id"=>1}}])
    a.each do |id|
      doc = @retailer_products.find(id).first
      doc["dress_codes"] << {"name"=>"festive","score"=>100}
      @occasions_mapped_products<<doc
    end
    @rows_groups =  Hash.new
    @items_groups = Hash.new
    @group_keys_hash = Hash.new
    @group_keys_negative = Hash.new
  end


  def get_parent_type product_type
    parent_type = nil
    begin
      doc = @product_types.find({"name"=>product_type,"vertical"=>"fashion"},{'direct_parent'=>1,"_id"=>0}).first
      if doc.nil?
        doc =@product_types_uk.find({"name"=>product_type,"vertical"=>"fashion"},{'direct_parent'=>1,"_id"=>0}).first
        parent_type = doc["direct_parent"]
      else
        parent_type= doc["direct_parent"]
      end
    rescue Exception => e
      puts "can't find parent product for #{product_type}"
    end
    parent_type
  end


  def combine_to_single_dress_code dress_codes_array
    if dress_codes_array.nil? or dress_codes_array.empty?
      return nil
    end
    dress_codes_array = dress_codes_array.select { |hash| hash["name"] if hash["score"] == 100 }
    if dress_codes_array.nil? or dress_codes_array.empty?
      return nil
    end
    dress_codes_array = dress_codes_array.map { |hash| hash["name"]  }
    dress_codes_array.sort!
    single_dress_code  = dress_codes_array.join("_")
    [dress_codes_array,single_dress_code]
  end


  def resolve_colors_and_groups color_array
    colors = []
    color_groups=[]
    color_array.each_with_index  do |hash,i|
      val = hash["main_synonym"].to_s
      if val.nil? || val.empty?
        val = hash["lemma"].downcase
      end

      if @color_names.include?val
        colors<< val
      end
      color_intersect = @color_names & hash["contained_groups"]
      colors = (colors + color_intersect).uniq
      color_groups_intersect = COLOR_GROUPS & hash["contained_groups"]
      color_groups = (color_groups + color_groups_intersect).uniq
    end
    [colors,color_groups]
  end


  def resolve_general_aspect aspect_array
    vals = []
    aspect_array.each_with_index  do |hash,i|
      val = hash["main_synonym"].to_s.downcase
      if val.nil? || val.empty?
        val = hash["lemma"].downcase
      end

      vals<<val
    end
    vals
  end

  # data for market basket
  def generate_items row
    items_array=[]
    row.each do |key,val|
      next if key == "gender"
      next if key == "dress_codes_array"
      next if key == "parent_type"
      next if key == "id"
      if val.is_a?(Array)
        items_array += val.map{|item| key+'_'+item}
      else
        items_array<<key+'_' + val
      end
    end
    items_array.uniq!
    items_array
  end

# generate doc for decision tree
  def generate_doc row
    arr=[]
    row.each do |key,val|
      next if key == "gender"
      next if key == "dress_codes_array"
      next if key == "parent_type"
      next if key == "id"
      next if key == "dress_code"
      if val.is_a?(Array)
        arr += val.map{|item| key+'-'+item}
      else
        arr<<key+'-' + val
      end
    end
    arr.uniq!
    arr
  end


  def gen_key_hash row
    key_hash = Hash.new
    key_hash["parent_type"]=row["parent_type"]
    key_hash["gender"]=row["gender"]
    key_hash["dress_codes_array"] = row["dress_codes_array"]
    key_hash
  end

  def generate_group_key row
    dress_code = row["dress_code"]
    group_key_hash=nil
    group_key = [row["parent_type"],row["gender"],'dress_code'+'_'+dress_code+""].join("#")
    if ! @group_keys_hash.key?group_key
      @group_keys_hash[group_key]= gen_key_hash row
    end
    group_key
  end


  def generate_negative_keys
    @group_keys_hash.each do |key,val|
      @group_keys_negative[key]=[]
      @group_keys_hash.each do |key2,val2|
        next if key2==key
        next if val2["gender"]!=val["gender"]
        next if val2["parent_type"]!=val["parent_type"]
        next if !(val2["dress_codes_array"] & val["dress_codes_array"] ).empty?
        @group_keys_negative[key]<<key2
      end
    end
  end


  def insert_into_rows_group group_key,items,doc
    if ! @rows_groups.key? group_key
      @rows_groups[group_key]=[]
    end
    if ! @items_groups.key? group_key
      @items_groups[group_key]=[]
    end
    @items_groups[group_key] << items
    @rows_groups[group_key] << doc
  end


  #csv for market basket analysis
  def generate_items_set_csv_files
    @items_groups.each do |key,rows|
      total_rows =[]
      total_rows +=rows
      neg_keys = @group_keys_negative[key]

      neg_keys.each do |neg_key|
        neg_rows = @items_groups[neg_key]
        total_rows+=neg_rows
      end

      filename = [key,"csv"].join(".")
      CSV.open(ITEMS_DIRECTORY+"/"+filename, "w", :write_headers => false) do |csv|
        total_rows.each do |row|
          csv << row
        end
      end
    end
  end

  #csv for decision tree
  def generate_docs_csv_files
    @rows_groups.each do |key,rows|
      pos_rows =rows
      neg_keys = @group_keys_negative[key]
      neg_rows = []
      neg_keys.each do |neg_key|
        neg_rows += @rows_groups[neg_key]
      end
      filename = [key,"csv"].join(".")
      headers = ["doc","class"]
      CSV.open(DOCS_DIRECTORY+"/"+filename, "w", :write_headers => false) do |csv|
        csv<<headers
        pos_rows.each do |row|
          row_no_dress_code = row.reject{|s| s.include?"dress_code"}
          row_no_dress_code = row_no_dress_code.map{|s| s.gsub(' ',"_")}
          str_row = row_no_dress_code.join(" ")
          pos_row = [str_row,1]
          csv << pos_row
        end
        neg_rows.each do |row|
          row_no_dress_code = row.reject{|s| s.include?"dress_code"}
          row_no_dress_code = row_no_dress_code.map{|s| s.gsub(' ',"_")}
          str_row = row_no_dress_code.join(" ")
          neg_row = [str_row,0]
          csv << neg_row
        end
      end
    end
  end

  # normalize dress code
  def get_main_dress_code_from_synonym dress_code_synonym
    cur = @dress_codes_tags.find('synonyms'=>dress_code_synonym)
    cur.each do |doc|
      return doc["dress_code"]
    end
    dress_code
  end


  def pre_process
    query = {"subverticals"=>"clothing","age_group"=>/^Adult$/i,"dress_codes.0"=>{"$exists"=>true},"dress_codes.1"=>{"$exists"=>false},"dress_codes.0.score"=>100}
    docs =  @retailer_products.find(query)
    occasions_docs = @occasions_mapped_products
    iters = [docs,occasions_docs]
    iters.each do |docs|

    docs.each_with_index do |doc,i|
      begin
        print "\r"+ i.to_s + ":"+doc["_id"]
        row =  Hash.new
        row["id"] = doc["_id"]
        row["gender"] = doc[:gender].downcase
        row["product_type"] = doc["product_type"].downcase
        row["parent_type"] = get_parent_type row["product_type"]
        next if row["parent_type"] != "shirt" || row["product_type"]!="shirt"
        #row["_id"]=doc["_id"]
        dress_codes_array,resolved_dress_code = combine_to_single_dress_code doc["dress_codes"]

        #no dress code found or low score
        if resolved_dress_code.nil?
          next
        end

        row["dress_code"] = resolved_dress_code
        row["dress_code"] = get_main_dress_code_from_synonym resolved_dress_code
        row["dress_codes_array"] = dress_codes_array

        for aspect_doc in doc["desc_aspects"]
          aspect_type = aspect_doc["aspect"]

          # do not include those aspects in dataset
          next if aspect_type.include? "size"
          next if aspect_type.include? "weather"
          next if aspect_type.include? "price"
          next if aspect_type.include? "body type"

          #normalize color
          if aspect_type.include? "color"
            colors,color_groups = resolve_colors_and_groups aspect_doc["desc_words"]
            row["color"]=colors
            row["color_group"]=color_groups
            next
          end

          #process aspects in desc words
          if aspect_doc["desc_words"].size > 0
            aspect_values = resolve_general_aspect aspect_doc["desc_words"]
            row[aspect_type] = aspect_values
            next
          end
        end

        items = generate_items row
        doc = generate_doc row
        group_key = generate_group_key row
        insert_into_rows_group  group_key,items,doc

      rescue Exception =>e
        puts '========================='
        puts e
        puts '========================='
      end
    end   # all products
    end

    generate_negative_keys
    generate_docs_csv_files
  end

  def self.humanize_rules
    rules_dir = "data_dt_rules/"
    files = Dir.glob("data_dt_rules/*.csv")
    puts files
    product_types = []
    features = []
    rules=[]
    current_class = "0"
    prob = nil

    CSV.open("dress_codes_rules_humanized/dress_codes_rules.csv","w") do |csv|
    files.each do |f|
      path,parent_type,gender,dress_code = f.split("#")
      dress_code=dress_code.gsub("_"," ").capitalize!
      csv << ["Rule","Prob","Manual Label",dress_code,parent_type.capitalize,gender.capitalize]
      group = f.split(/[\/.]/)[3]
      File.read(f).each_line do |line|
        line.strip!
         next if line.empty?
         feature = ""
         if line.include?"Rule"
           #positive group
           if current_class == "1"
             #push negative features to be the last
             features=features.sort{|w1,w2| ((w1=~(/(not)/))).to_s <=> ((w2=~(/(not)/))).to_s }
             pos =  product_types.reject{|p| p.match(/^(not)/)}
             pos_features = features.reject{|p| p.match(/(not)/)}
             if !pos.empty? || !pos_features.empty?
               features = pos + features
               rules << [group,features.join(",")]
               csv<< [features.join(","),prob,"",""]
             else
               puts 'non significant rule'
             end
             features=[]
             product_types = []
           end

           matches = line.match(/\[(.?+*)\]/)
           a= matches.captures
           d=line.match(/(class=)([0-9])/)
           a,b =  d.captures
           current_class=b

           next if current_class == "0"
           d=line.match(/(prob=)([0-9].[0-9])/)
           prob =  d.captures[1]
           next

         end

         next if current_class == "0"
         feature_not=""

         if true
           if line.include?"<"
             feature = line.split("<")[0]
             feature_not = "not "
           elsif line.include?">="
             feature = line.split(">=")[0]
           end
         end
         feature_type,feature_val = feature.split("-",2)
         if !feature.empty?
           feature_val= feature_val.gsub("_"," ")
           if feature_type.include?"product_type"
             product_types << feature_not+feature_val
             else
              features<<feature_type+" "+feature_not+feature_val
             end
         end
      end
      csv<< ["","","",""]
      csv<< ["","","",""]
      csv<< ["","","",""]
    end
    end
    puts '==============================='
    puts rules
  end

end

Rules.new().pre_process
#Rules::humanize_rules