# transcode-transcribe/Dockerfile
# Base image with Cloud Functions (Gen2) bootstrap for Node 20
FROM gcr.io/functions/base:nodejs20

# Install ffmpeg (lean install)
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Functions base expects code in /workspace
WORKDIR /workspace

# Install deps first for better caching
COPY package*.json ./
RUN if [ -f package-lock.json ]; then npm ci --only=production; else npm install --omit=dev; fi

# Copy the rest of the sources
COPY . .

# No CMD/ENTRYPOINT: Functions base boots your exported entry point.
