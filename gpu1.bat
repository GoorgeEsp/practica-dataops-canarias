curl http://gpu1.esit.ull.es:4000/v1/chat/completions \ ^
  -H "Content-Type: application/json" ^
  -H "Authorization: Bearer sk-1234" ^
  -d "{ \"model\": \"ollama/llama3.1:8b\", \"messages\": [{\"role\":\"user\",\"content\":\"Hola\"}] }"