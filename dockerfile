FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the user_login folder, main.py and requirements.txt into the container's working directory
COPY ./user_login ./user_login
COPY ./requirements.txt ./requirements.txt
COPY ./main.py ./main.py

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the Python script
CMD ["python", "main.py"]
