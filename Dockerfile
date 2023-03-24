FROM python:3.8

# Install python packages
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Create a new directory named "myapp"
RUN mkdir /pse-stocks-etl

# Set the working directory to the new directory
WORKDIR /pse-stocks-etl

# Copy source code files
COPY src src
COPY tests tests

CMD ["python", "-m", "src.etl.sync"]