# Use the official Python base image with Python 3.8
FROM python:3.8

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file to the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code to the container
COPY Src/ /app/Src/

# Copy the data folder to the container
COPY Data/ /app/Data/

# Set the entry point for the container
CMD [ "python", "/app/Src/Data/main.py" ]
