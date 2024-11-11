import json

# Load user-login schema
with open('user_login/schemas/user_login.json', 'r') as schema_file:
    USER_LOGIN_SCHEMA = json.load(schema_file)
