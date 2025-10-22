# Use Artifact Registry base image for Node 20 (serverless runtimes)
# Choose -full if you prefer more system packages available
FROM us-central1-docker.pkg.dev/serverless-runtimes/google-22-full/runtimes/nodejs20

# Install ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Install deps first for better caching
COPY package*.json ./
RUN if [ -f package-lock.json ]; then npm ci --only=production; else npm install --omit=dev; fi

# Copy the rest of your sources
COPY . .

# Cloud Functions Gen2 via Functions Framework (CloudEvents)
ENV FUNCTION_TARGET=onAudioUploaded
ENV FUNCTION_SIGNATURE_TYPE=cloudevent

# Start Functions Framework (uses $PORT automatically)
CMD ["npx", "functions-framework"]
