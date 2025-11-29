#from dash import Dash
#from frontend.layout import layout
#from frontend.callbacks import register_callbacks
#from backend.slave_gpt import generate_geopolitical_summary
#from backend.data import df_countries
from backend.input_handler import update_corpus

if __name__ == "__main__":
    print("Select mode : \n1) Update RAG database\n2) Update GPT generated prompts\n3) Launch App")
    option = input()
    if option == '1':
        print("Webscaping and storing info into the RAG database")
        update_corpus()
        #df_countries.loc[df_countries['country'] =='Algeria'] = 
        #print(generate_geopolitical_summary(country="Algeria"))
        #print(df_countries['Algeria'])
        #print("fetch1")
        #pages_content = fetch_wikipedia_pages("Algeria")
        #print("Add2")
        #add_country_to_db(pages_content)
#    elif option =='2':
#        print("Updating GPT Generated prompts")
#    else:
#        app = Dash(__name__)
#        app.title = "Interactive World Map"
#        app.layout = layout
    
#        register_callbacks(app)
#        app.run(debug=False)
