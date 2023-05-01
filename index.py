import os
import openai
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

apikey = os.getenv("OPENAI_KEY")
print(apikey)
openai.api_key = os.getenv(f'{apikey}')

df = pd.read_csv('sales_data_sample.csv')
# print(df.groupby('QTR_ID').sum()['SALES'])

temp_db = create_engine('sqlite:///:memory:', echo=True)
data = df.to_sql(name='Sales', con=temp_db)

with temp_db.connect() as conn:
    result = conn.execute(text("SELECT SUM(SALES) FROM Sales"))


# print(result.all())


def create_table_definition(df):
    prompt = """### sqlite SQL table, with it properties:
    #
    # Sales({})
    #
    """.format(",".join(str(col) for col in df.columns))

    return prompt


# print(df.columns)


def prompt_input():
    nlp_text = input("Enter the info you want:")
    return nlp_text


# prompt_input()


def combine_prompts(df, query_prompt):
    definition = create_table_definition(df)
    query_init_string = f"### A query to answer : {query_prompt}\nSELECT"
    return definition + query_init_string


openai.api_key = f"{apikey}"

nlp_text = prompt_input()
# print(combine_prompts(df, nlp_text))
response = openai.Completion.create(
    model='gpt-3.5-turbo',
    prompt=combine_prompts(df, nlp_text),
    temperature=0,
    max_tokens=150,
    top_p=1.0,
    frequency_penalty=0,
    presence_penalty=0,
    stop=['#', ';']
)
print(response['choices'][0]['text'])


def handle_response(response):
    query = response['choices'][0]['text']
    if query.startwith(" "):
        query = "SELECT" + query
        return query


handle_response(response)

with temp_db.connect() as conn:
    result = conn.execute(text(handle_response(response)))

result.all()
