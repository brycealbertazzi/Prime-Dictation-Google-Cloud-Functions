# Artifact Registry base image (serverless runtime for Node 20)
FROM us-central1-docker.pkg.dev/serverless-runtimes/google-22-full/runtimes/nodejs20

# Switch to root to install OS packages
USER root

# Install ffmpeg
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg \
 && rm -rf /var/lib/apt/lists/*

# Drop back to nonroot user (the serverless images use UID 65532)
USER 65532

WORKDIR /workspace

# Install deps first (better caching)
COPY package*.json ./
RUN if [ -f package-lock.json ]; then npm ci --only=production; else npm install --omit=dev; fi

# Copy sources
COPY . .

# Functions Framework (HTTP server) config
ENV FUNCTION_TARGET=onAudioUploaded
ENV FUNCTION_SIGNATURE_TYPE=cloudevent

# Start the Functions Framework (uses $PORT automatically)
CMD ["npx", "@google-cloud/functions-framework", "--target=onAudioUploaded", "--signature-type=cloudevent"]
