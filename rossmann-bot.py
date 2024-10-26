import os
import requests
import json
import pandas as pd
from flask import Flask, request, Response
from threading import Thread

# Token do Bot no Telegram
TOKEN = os.environ.get('TOKEN')

def send_message(chat_id, text):
    url = f'https://api.telegram.org/bot{TOKEN}/'
    url = url + f'sendMessage?chat_id={chat_id}' 

    r = requests.post(url, json={'text': text})
    print('Status Code {}'.format(r.status_code))

    return None

def load_dataset(store_id):
    # loading test dataset
    df10 = pd.read_csv('datasets/test.csv')
    df_store_raw = pd.read_csv('datasets/store.csv')

    # merge test dataset + store
    df_test = pd.merge(df10, df_store_raw, how='left', on='Store')

    # choose store for prediction
    df_test = df_test[df_test['Store'] == store_id]

    if not df_test.empty:
        # remove closed days
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop('Id', axis=1)

        # convert Dataframe to json
        data = json.dumps(df_test.to_dict(orient='records'))

    else:
        data = 'error'

    return data

def wake_up_application():
    # Envia uma requisição para acordar a aplicação
    response = requests.get('https://api-rossmann-edinan-marinho.onrender.com')
    return response.status_code == 200  # Retorna True se o status for 200

def predict(data):
    # Faz o wake-up e aguarda o retorno
    if not wake_up_application():
        print("Falha ao acordar a aplicação.")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de falha

    # Chamada para o endpoint de previsão
    url = 'https://api-rossmann-edinan-marinho.onrender.com/rossmann/predict'
    header = {'Content-type': 'application/json'}
    r = requests.post(url, data=data, headers=header)

    # Converte o resultado para DataFrame
    if r.status_code == 200:
        d1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())
        return d1
    else:
        print(f"Erro na previsão: {r.status_code}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro

def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']

    store_id = store_id.replace('/', '')

    try:
        store_id = int(store_id)
    except ValueError:
        store_id = 'error'

    return chat_id, store_id

# API initialize
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        message = request.get_json()

        chat_id, store_id = parse_message(message)

        if store_id != 'error':
            # loading data
            data = load_dataset(store_id)

            if data != 'error':
                # prediction
                d1 = predict(data)

                if not d1.empty:
                    # calculation
                    d2 = d1[['store', 'prediction']].groupby('store').sum().reset_index()
                    
                    # send message
                    msg = 'Loja número {} venderá ${:,.2f} nas próximas 6 semanas'.format(
                        d2['store'].values[0],
                        d2['prediction'].values[0]
                    ) 

                    send_message(chat_id, msg)
                    return Response('Ok', status=200)
                else:
                    send_message(chat_id, 'Erro ao realizar a previsão.')
                    return Response('Ok', status=200)

            else:
                send_message(chat_id, 'Loja não disponível')
                return Response('Ok', status=200)

        else:
            send_message(chat_id, 'ID Loja errado')
            return Response('Ok', status=200)

    else:
        return '<h1> Rossmann Telegram BOT </h1>'

if __name__ == '__main__':
    port = os.environ.get('PORT', 5000)
    app.run(host='0.0.0.0', port=port)
