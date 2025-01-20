import time
from datetime import datetime
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

    access_token = 'Basic Y29uY2VpdG86R0dHNiBjaTZzIDdCbm4gSUVQbCAzSXl6IHVYeWo='
    my_headers = {'Authorization': f'{access_token}'}
    apiURL = f'https://imnd.com.br/api/automation/appointments?page=1&limit=30000&date_start=2024-10-15&date_end=2024-10-30'

    requisicao = requests.get(apiURL, headers=my_headers)

    if requisicao.status_code == 200:
        data = requisicao.json()
        
        # Contagem de status
        status_counts = Counter(node["status"] for node in data["nodes"])
        
        # Filtrar registros conforme critérios
        realizados_aprovados = []
        realizados_nao_aprovados = []
        
        for node in data["nodes"]:
            if node["status"] == "Realizado":
                ts_status = node.get("metas", {}).get("ts_status", None)
                if ts_status == "APROVADO":
                    realizados_aprovados.append(node)
                else:
                    realizados_nao_aprovados.append(node)

        # Exibir os resultados da contagem de status
        print("\nContagem por status:")
        for status, count in status_counts.items():
            print(f"{status}: {count}")

        # Exibir registros filtrados
        print("\n📌 Registros com Status 'Realizado' e ts_status 'APROVADO':")
        for item in realizados_aprovados:
            print(item)

        print("\n📌 Registros com Status 'Realizado' e ts_status diferente de 'APROVADO':")
        for item in realizados_nao_aprovados:
            print(item)

        # Atualizar as tags na API
        update_tag("IMND_MES_ATUAL_REALIZADOS_APROVADOS", len(realizados_aprovados))
        update_tag("IMND_MES_ATUAL_REALIZADOS_NAO_APROVADOS", len(realizados_nao_aprovados))

    else:
        print(f"Erro {requisicao.status_code}")

    print(f"Tarefa executada às {datetime.now()}")

if __name__ == "__main__":
    loadIMNDData()
