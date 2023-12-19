# Use an official PySpark image as a parent image
FROM jupyter/pyspark-notebook:latest

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container at /app
COPY . /app/

# Specify the command to run and accept command-line arguments
CMD ["python", "./app/__main__.py"]
