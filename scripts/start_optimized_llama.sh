#!/bin/bash

# GTX 1650 optimized llama.cpp server script
# 4GB VRAM, i5 12th gen, 16GB RAM

MODEL_PATH="/home/aymen/Desktop/Models/gemma-3-1b-it-q4_0.gguf"

# GPU memory optimization
export CUDA_VISIBLE_DEVICES=0
export CUDA_CACHE_DISABLE=0

echo "=== System Info ==="
echo "GPU: GTX 1650 (4GB VRAM)"
echo "CPU: i5 12th gen (12 cores)"
echo "RAM: 16GB"
echo "==================="

# Check GPU memory before starting
nvidia-smi --query-gpu=memory.used,memory.free,memory.total --format=csv,noheader,nounits

echo "Starting optimized llama.cpp server..."

# Start with optimized settings and minimal HTTP logging
llama-server \
  --model "$MODEL_PATH" \
  --host 0.0.0.0 \
  --port 8080 \
  --ctx-size 4096 \
  --temp 0.9 \
  --n-predict -1 \
  --threads 10 \
  --n-gpu-layers 15 \
  --batch-size 512 \
  --ubatch-size 128 \
  --flash-attn \
  --cache-type-k f16 \
  --cache-type-v f16 \
  --mlock \
  --cont-batching \
  --parallel 4 \
  --defrag-thold 0.1 \
  --verbosity 1 \
  --log-file http_requests.log \
  --log-timestamps \
  --log-prefix

