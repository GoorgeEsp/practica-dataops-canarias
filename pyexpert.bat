curl http://gpu1.esit.ull.es:4000/v1/chat/completions ^
  -H "Content-Type: application/json" ^
  -H "Authorization: Bearer sk-1234" ^
  -d "{ \"model\": \"ollama/llama3.1:8b\", \"messages\": [{\"role\": \"system\", \"content\": \"Eres un experto en Python. Vas a crear solo codigo python.\"}, {\"role\": \"user\", \"content\": \"Como hacer un bucle for\"}],\"temperature\": 0.7, \"max_tokens\": 500,\"top_p\": 0.9}"