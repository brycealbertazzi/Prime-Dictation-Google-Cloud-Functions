# Artifact Registry serverless runtime base (Node 20)
FROM us-central1-docker.pkg.dev/serverless-runtimes/google-22-full/runtimes/nodejs20

# --- run privileged steps as root ---
USER root

# OS deps
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg \
 && rm -rf /var/lib/apt/lists/*

# App dir
WORKDIR /workspace

# Install Node deps as root (so we can write node_modules)
COPY package*.json ./
RUN if [ -f package-lock.json ]; then npm ci --only=production; else npm install --omit=dev; fi

# Copy sources
COPY . .

# Make runtime user own the files
RUN chown -R 65532:65532 /workspace

# --- drop privileges for runtime ---
USER 65532

# Functions Framework config (CloudEvents)
ENV FUNCTION_TARGET=onAudioUploaded
ENV FUNCTION_SIGNATURE_TYPE=cloudevent

# Start the Functions Framework (Cloud Run/Functions inject $PORT)
CMD ["npx", "@google-cloud/functions-framework", "--target=onAudioUploaded", "--signature-type=cloudevent"]
