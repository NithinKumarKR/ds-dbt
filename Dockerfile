FROM m2pfintech01/airbyte:dbt-1.4
ADD /dbt_project.yml /usr/app

# Use the official Python 3.10 slim image as a base
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /usr/app

# Install necessary system packages including build tools and libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    gcc \
    libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Copy the requirements.txt file to the container
COPY requirements.txt /usr/app/

# Install the required Python packages from requirements.txt
RUN pip install --timeout=500 --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . /usr/app/

# Specify the command to run the application when the container starts
CMD ["dbt", "--version"]
>>>>>>> Stashed changes
