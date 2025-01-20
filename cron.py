import time
from datetime import datetime
import requests

def loadIMNDData():
  print(f"Iniciando tarefa às {datetime.now()}")
  
  access_token = 'Basic Y29uY2VpdG86R0dHNiBjaTZzIDdCbm4gSUVQbCAzSXl6IHVYeWo='
  my_headers = {'Authorization': f'{access_token}'}
  apiURL = f'https://imnd.com.br/api/automation/appointments?page=1&limit=30000&date_start=2024-10-15&date_end=2024-10-30'
  
  requisicao = requests.get(apiURL, headers=my_headers)

  if requisicao.status_code == 200:
    data = requisicao.json()
    print(data)
  else:
    print(f"Erro {requisicao.status_code}")
  
  print(f"Tarefa executada às {datetime.now()}")

if __name__ == "__main__":
  loadIMNDData()
