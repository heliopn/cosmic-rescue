import json
import psycopg2
import boto3
import logging

model_file = '/opt/ml/model'  # Path to model file

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
def predict_transported(passenger_id):
    logging.info('Predict: %s', passenger_id)
    # BYPASS TEMPORARIO
    # LOGICA DE PREDICT AQUI
    return True

# PostgreSQL Connection and Operations
def postgres_operations(passenger_id):

    # Get secrets from AWS Secrets Manager
    secret_name = 'db_credencials'
    secret_data = get_secret(secret_name)

    # Extract database credentials
    db_user = secret_data['username']
    db_password = secret_data['password']
    db_host = secret_data['host']
    db_port = secret_data['port']
    db_name = secret_data['dbname']


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
        predicted_transported = predict_transported(passenger_id)

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
    logging.info('Received event: %s', event)
    try:
        # Extract PassengerId from the API input
        body = json.loads(event['body'])
        passenger_id = body.get('passengerid')

        # Perform operations in PostgreSQL
        result = postgres_operations(passenger_id)

        return {
            'statusCode': 200,
            'body': json.dumps(passenger_id)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
