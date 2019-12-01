




df = pd.read_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/multi/prepare_data/df.csv',index_col=0)



df_raw = pd.read_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/multi/prepare_data/df_raw.csv',index_col=0)



when back, why df and dfp raw are different in size?? what do i use to create this multi dataset ?


aspects = ['nbr_response_code_replaced_color_question',
           'nbr_response_code_replaced_fabric_question',
           'nbr_response_code_replaced_fit_question',
           'nbr_response_code_replaced_neckline_question',
           'nbr_response_code_replaced_sleeve style_question',
           'nbr_response_code_replaced_style_question',
           ]


# get aspect questions
subset = df[df[aspects].any(axis="columns")]

# remove columns that are all zeroes
subset1 = subset.loc[:, (subset != 0).any(axis=0)]





subset1.to_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/aspect_questions/df.csv')


# get oly answered

subset2 = subset1[subset1["is_answered"]==1]

subset_no_answered = subset1[subset1["is_answered"]==0]


subset2.to_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/multi/df_nulti.csv')


subset2["label"] = subset2[aspects].idxmax(axis=1)

subset_no_answered["label"] = subset_no_answered[aspects].idxmax(axis=1)

subset_no_answered["label_int"]

from sklearn.preprocessing import LabelEncoder  


le = LabelEncoder()
subset2["label_int"] = le.fit_transform(subset2["label"])

subset_no_answered["label_int"] = le.transform(subset_no_answered["label"])


maps = le.inverse_transform([0,1,2,3,4,5])

x_col = [ 'question_rank',
 'time_from_start',
 'sum_answer',
 'num_quick_replies',
 'hour',
 'day_of_week',
 'last_nbr_code_replaced_children_types_question',
 'last_nbr_code_replaced_color_question',
 'last_nbr_code_replaced_consumer_mismatch',
 'last_nbr_code_replaced_customer_care_url',
 'last_nbr_code_replaced_fabric_question',
 'last_nbr_code_replaced_fit_question',
 'last_nbr_code_replaced_no_products_for_type',
 'last_nbr_code_replaced_offer_other_products_for_type',
 'last_nbr_code_replaced_offer_other_products_for_type_parent',
 'last_nbr_code_replaced_other',
 'last_nbr_code_replaced_product_type_selection',
 'last_nbr_code_replaced_shopping_open_question_no_gender',
 'last_nbr_code_replaced_sleeve style_question',
 'last_nbr_code_replaced_subvertical_selection',
 'last_nbr_code_replaced_subvertical_selection_second',
 'last_nlu_intents_list_replaced__Help, Shop Now, neutral_',
 'last_nlu_intents_list_replaced__New Search, neutral_',
 'last_nlu_intents_list_replaced__Not Understood, neutral_',
 'last_nlu_intents_list_replaced__Shop Now, Indifferent_',
 'last_nlu_intents_list_replaced__Shop Now, neutral_',
 'last_nlu_intents_list_replaced__Shop Now, positive_',
 'last_nlu_intents_list_replaced__neutral, Shop Now_',
 'last_nlu_intents_list_replaced__positive, Shop Now_',
 'last_nlu_intents_list_replaced__positive_',
 'last_nlu_intents_list_replaced_other',
 'last_nlu_subvertical_replaced_accessories',
 'last_nlu_subvertical_replaced_bags',
 'last_nlu_subvertical_replaced_clothing',
 'last_nlu_subvertical_replaced_footwear',
 'last_nlu_subvertical_replaced_lounge \\u0026 underwear',
 'last_nlu_subvertical_replaced_other',
 'last_nlu_subvertical_replaced_outerwear',
 'last_nlu_subvertical_replaced_watches',
 'last_nlu_positive_aspects_replaced___',
 'last_nlu_positive_aspects_replaced__body type_',
 'last_nlu_positive_aspects_replaced__color_',
 'last_nlu_positive_aspects_replaced__cut_',
 'last_nlu_positive_aspects_replaced__fabric_',
 'last_nlu_positive_aspects_replaced__fit_',
 'last_nlu_positive_aspects_replaced__length_',
 'last_nlu_positive_aspects_replaced__neckline_',
 'last_nlu_positive_aspects_replaced__price_',
 'last_nlu_positive_aspects_replaced__size uk, size eu, size us_',
 'last_nlu_positive_aspects_replaced__size uk, size uk, size us, size, size us, size eu, size eu_',
 'last_nlu_positive_aspects_replaced__size uk, size uk, size, size us, size us, size eu, size eu_',
 'last_nlu_positive_aspects_replaced__size_',
 'last_nlu_positive_aspects_replaced__sleeve style_',
 'last_nlu_positive_aspects_replaced__weather_',
 'last_nlu_positive_aspects_replaced_other',
 'last_nlu_positive_product_type_replaced_athletic shoes',
 'last_nlu_positive_product_type_replaced_boots',
 'last_nlu_positive_product_type_replaced_cap',
 'last_nlu_positive_product_type_replaced_coat',
 'last_nlu_positive_product_type_replaced_dress',
 'last_nlu_positive_product_type_replaced_hoodie',
 'last_nlu_positive_product_type_replaced_jacket',
 'last_nlu_positive_product_type_replaced_jeans',
 'last_nlu_positive_product_type_replaced_other',
 'last_nlu_positive_product_type_replaced_pants',
 'last_nlu_positive_product_type_replaced_shirt',
 'last_nlu_positive_product_type_replaced_shorts',
 'last_nlu_positive_product_type_replaced_sweater',
 'last_nlu_positive_product_type_replaced_sweatshirt',
 'last_nlu_positive_product_type_replaced_underwear',
 'last_nlu_positive_product_type_replaced_vest',
 'last_nlu_positive_product_type_replaced_watch',
 'last_nlu_gender_replaced_female',
 'last_nlu_gender_replaced_male',
 'last_nlu_gender_replaced_other',
 'last_nlu_gender_replaced_unisex',
 'last_nlu_gender_replaced_user',
 'last_nlu_age_group_replaced_adult',
 'last_nlu_age_group_replaced_kids',
 'last_nlu_age_group_replaced_other',
 'last_nlu_age_group_replaced_user',
 'customer_type_replaced_NULL',
 'customer_type_replaced_New Visitor',
 'customer_type_replaced_Returning Visitor',
 'customer_type_replaced_other',
 'platform_replaced_Desktop',
 'platform_replaced_Mobile',
 'platform_replaced_other']

y_col = ["label_int"]







X_train, X_test, y_train, y_test = train_test_split(subset2[x_col], subset2[y_col], test_size=0.2, random_state=42,stratify = subset2[y_col])






param = {"objective":'multi:softprob',
         
                                 "eval_metric":"mlogloss",
                                 "num_class":6,
                                 "colsample_bytree":1,
                                 "learning_rate":0.03,
                                 "max_depth":6,
                                 "subsample":0.8,
                                 "base_score":0.22,
                                 "seed":2,
                                 }
    
num_round = 300


dtrain = xgb.DMatrix(X_train,y_train)
dtest = xgb.DMatrix(X_test,y_test)

watchlist = [ (dtrain,'train'), (dtest, 'test') ]

evals_result = {}
        
bst = xgb.train(param,dtrain,num_round,evals = watchlist,early_stopping_rounds=50, evals_result = evals_result)


importance(bst)


y_pred = bst.predict( dtest )


y_pred_label = np.argmax(y_pred, axis=1)




from sklearn.metrics import confusion_matrix
from sklearn.metrics import multilabel_confusion_matrix
from sklearn.metrics import classification_report


confusion_matrix(y_test, y_pred_label)

print("report on answer")
print(classification_report(y_test, y_pred_label, target_names=maps))


# check on no answer

subset_no_answered["pred"] = np.argmax(bst.predict( xgb.DMatrix(subset_no_answered[x_col]) ), axis=1)

print("report on no answer")
print(classification_report(subset_no_answered[y_col], subset_no_answered["pred"], target_names=maps))


# when back check all that i have done here !






