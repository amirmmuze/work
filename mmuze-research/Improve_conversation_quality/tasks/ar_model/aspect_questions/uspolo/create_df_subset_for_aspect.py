




df = pd.read_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/all_questions/uspolo/prepare_data_res/df.csv',index_col=0)



aspects = ['nbr_response_code_replaced_color_question',
           'nbr_response_code_replaced_fabric_question',
           'nbr_response_code_replaced_fit_question',
           'nbr_response_code_replaced_neckline_question',
           'nbr_response_code_replaced_sleeve style_question',
           'nbr_response_code_replaced_style_question',
           ]


 
subset = df[df[aspects].any(axis="columns")]

subset1 = subset.loc[:, (subset != 0).any(axis=0)]


subset1.to_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/aspect_questions/df.csv')