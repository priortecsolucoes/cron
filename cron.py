import time
from datetime import datetime, date
import calendar
import requests
from collections import Counter

def update_tag(tag_name, int_value):
    """ Envia uma requisição PUT para atualizar uma tag na API """
    url = "https://fastapi-production-1598.up.railway.app/update-tag"
    headers = {"Content-Type": "application/json"}
    
    body = {
        "tag_name": tag_name,
        "string_value": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "int_value": int_value,
        "double_value": 0
    }
    
    response = requests.put(url, json=body, headers=headers)
    
    if response.status_code == 200:
        print(f"✅ Tag '{tag_name}' atualizada com sucesso! Valor: {int_value}")
    else:
        print(f"❌ Erro ao atualizar a tag '{tag_name}'. Código: {response.status_code}, Resposta: {response.text}")

def loadIMNDData():
    print(f"Iniciando tarefa às {datetime.now()}")

    # Calculando as datas para a API (primeiro dia do mês até o último dia do mês)
    first_day_of_month = date.today().replace(day=1)
    
    # Obtém o último dia do mês corrente
    last_day = calendar.monthrange(date.today().year, date.today().month)[1]
    last_day_of_month = date.today().replace(day=last_day)
    
    # Convertendo para o formato aceito pela API (YYYY-MM-DD)
    date_start = first_day_of_month.strftime("%Y-%m-%d")
    date_end = last_day_of_month.strftime("%Y-%m-%d")

    print(f"date_start = {date_start}")
    print(f"date_end = {date_end}")
    
    access_token = 'Basic Y29uY2VpdG86R0dHNiBjaTZzIDdCbm4gSUVQbCAzSXl6IHVYeWo='
    my_headers = {'Authorization': f'{access_token}'}
    
    # Atualizando a URL com as datas dinâmicas
    apiURL = f'https://imnd.com.br/api/automation/appointments?page=1&limit=30000&date_start={date_start}&date_end={date_end}'

    requisicao = requests.get(apiURL, headers=my_headers)

    if requisicao.status_code == 200:
        data = requisicao.json()
        
        # Contagem de status
        status_counts = Counter(node["status"] for node in data["nodes"])
        
        # Filtrar registros conforme critérios
        realizados_aprovados = []
        realizados_nao_aprovados = []
        pendentes = []  # Lista para armazenar registros com ts_status vazio

        for node in data["nodes"]:
            ts_status = node.get("metas", {}).get("ts_status", None)
            
            if node["status"] == "Realizado":                
                if ts_status == "APROVADO":
                    realizados_aprovados.append(node)
                else:
                    realizados_nao_aprovados.append(node)
            
            if ts_status is None or ts_status == "":
                pendentes.append(node)  # Adiciona à lista de pendentes

        # Exibir os resultados da contagem de status
        print("\nContagem por status:")
        for status, count in status_counts.items():
            print(f"{status}: {count}")

        # Atualizar as tags na API
        update_tag("IMND_MES_ATUAL_REALIZADOS_APROVADOS", len(realizados_aprovados))
        update_tag("IMND_MES_ATUAL_REALIZADOS_NAO_APROVADOS", len(realizados_nao_aprovados))
        update_tag("IMND_MES_ATUAL_PENDENTES", len(pendentes)) 

    else:
        print(f"Erro {requisicao.status_code}")

    print(f"Tarefa executada às {datetime.now()}")

if __name__ == "__main__":
    loadIMNDData()
