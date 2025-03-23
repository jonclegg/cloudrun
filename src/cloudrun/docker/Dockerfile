FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf \
        awscliv2.zip \
        aws \
        /var/lib/apt/lists/* \
        /var/cache/apt/*

# Install base Python packages
RUN pip install --no-cache-dir \
    boto3==1.34.69 \
    python-dotenv==1.0.1 \
    click==8.1.7

# Copy the package files
COPY src /app/src
COPY setup.py /app/
COPY pyproject.toml /app/

# Copy entrypoint script
COPY src/cloudrun/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Install additional requirements if they exist
COPY additional_requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/additional_requirements.txt && \
    rm /tmp/additional_requirements.txt

    # Install the package
RUN pip install .

ENTRYPOINT ["/entrypoint.sh"] 