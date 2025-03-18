# Use an official Python runtime as a parent image
FROM python:3

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install any needed dependencies specified in requirements.txt
RUN pip install -r requirements.txt

EXPOSE 3000

# Run the Python script when the container launches
CMD dagit -h 0.0.0.0 -f ETL_main.py