import ydf
import json
import boto3
import logging
import psycopg2
import pandas as pd
from postgresql_utils import get_cur, insert_predictions

if "model" not in os.listdir("./"):
    return {"error": "Model not found"}
rf = ydf.load_model("model")

def get_secret(secret_name):
    client = boto3.client('secretsmanager', region_name='your_region')

    try:
        response = client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response:
            secret = response['SecretString']
            return json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(response['SecretBinary'])
            return json.loads(decoded_binary_secret)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise

# Function to predict the Transported value
def predict_transported(df_person):#Has to reivei a df with the infos from the database
    logging.info('Predict: %s', df_person['passengerid'])

    # Add target column
    df_person['transported'] = None

    # Replace NaN values with zero
    df_person[['vip', 'cryosleep', 'foodcourt', 'shoppingmall', 'spa', 'vrdeck']] = df_person[['vip', 'cryosleep', 'foodcourt', 'shoppingmall', 'spa', 'vrdeck']].fillna(value=0)

    # Creating New Features - Deck, Cabin_num and Side from the column Cabin and remove Cabin
    df_person[["deck", "cabin_num", "side"]] = df_person["cabin"].str.split("/", expand=True)
    df_person = df_person.drop('cabin', axis=1)

    # Convert boolean to 1's and 0's
    df_person['vip'] = df_person['vip'].astype(int)
    df_person['cryosleep'] = df_person['cryosleep'].astype(int)

    # Get the predictions for testdata
    predictions = rf.predict(df_person)
    n_predictions = (predictions > 0.5).astype(bool).squeeze()
    return True

# PostgreSQL Connection and Operations
def postgres_operations(passenger_id):

    ### Reading environment variable
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_DATABASE")
    port = os.getenv("DB_PORT")

    ### Creates postgresql connection
    conn = psycopg2.connect(
        host=host,
        database=db_name,
        user=user,
        password=password,
        port=port)
    cursor = conn.cursor()

    # Check if PassengerId exists in the database
    cursor.execute("SELECT * FROM space_titanic_passengers WHERE passengerid = %s", (passenger_id))
    passenger_data = cursor.fetchone()

    logging.info('Select: %s', passenger_data)

    if not passenger_data:
        # PassengerId is invalid, return error message
        return {'error': 'Invalid PassengerId'}

    # Extract Transported column value
    transported_value = passenger_data['transported']

    if transported_value is None:
        # Predict Transported value using the model
        predicted_transported = predict_transported(passenger_data)

        # Store predicted value in PostgreSQL
        cursor.execute("UPDATE space_titanic_passengers SET transported = %s, predicted = %s WHERE passengerid = %s",
                       (predicted_transported, True, passenger_id))
        conn.commit()

        # Send message stating it's a prediction
        # Use AWS SNS, SQS, or any messaging service for sending the message

        return {'prediction': f'Transported = {predict_transported}'}
    else:
        # Passenger was transported
        return {'confirmation': 'Passenger was transported'}

    cursor.close()
    conn.close()

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)
    try:
        # Extract PassengerId from the API input
        body = json.loads(event['body'])
        passenger_id = body.get('passengerid')

        # Get the raw posted JSON
        logging.info('Received event raw input: %s', body)

        # Perform operations in PostgreSQL
        result = postgres_operations(passenger_id)

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
