import time
from datetime import datetime, date
import calendar
import requests
import os
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

def request_with_retries(url, headers, max_retries=2):
    """ Faz uma requisição com tentativas em caso de exceção """
    attempt = 0
    while attempt <= max_retries:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Levanta uma exceção para códigos de status HTTP de erro
            return response
        except Exception as e:
            attempt += 1
            print(f"⚠️ Tentativa {attempt} falhou: {e}")
            if attempt > max_retries:
                print("❌ Todas as tentativas falharam.")
                raise
            time.sleep(5)  # Aguarda 5 segundos antes de tentar novamente

def loadIMNDData():
    print(f"Iniciando tarefa às {datetime.now()}")

    # Calculando as datas para a API (primeiro dia do mês até o último dia do mês)
    first_day_of_month = date.today().replace(day=1)
    last_day = calendar.monthrange(date.today().year, date.today().month)[1]
    last_day_of_month = date.today().replace(day=last_day)
    
    # Convertendo para o formato aceito pela API (YYYY-MM-DD)
    date_start = first_day_of_month.strftime("%Y-%m-%d")
    date_end = last_day_of_month.strftime("%Y-%m-%d")
    
    print(f"date_start = {date_start}")
    print(f"date_end = {date_end}")
    
    access_token = os.getenv('IMND_ACCESS_TOKEN')
    if not access_token:
        raise EnvironmentError("A variável de ambiente 'IMND_ACCESS_TOKEN' não foi encontrada.")

    my_headers = {'Authorization': f'{access_token}'}
    
    # Inicializa variáveis de paginação
    page = 1
    has_more = True
    all_nodes = []
    
    while has_more:
        apiURL = f'https://imnd.com.br/api/automation/appointments?page={page}&status=scheduled,fulfilled,notaccomplished&limit=1000&date_start={date_start}&date_end={date_end}'
        print(f"🔄 Requisitando página {page}...")
        try:
            requisicao = request_with_retries(apiURL, my_headers)
            data = requisicao.json()
            all_nodes.extend(data.get("nodes", []))
            has_more = data.get("metadata", {}).get("pagination", {}).get("has_more", False)
            page += 1  # Incrementa para a próxima página
        except Exception as e:
            print(f"❌ Erro ao requisitar a página {page}: {e}")
            break  # Interrompe a execução em caso de erro

        time.sleep(10)
    
    # Contagem de status
    status_counts = Counter(node["status"] for node in all_nodes)
    
    # Filtrar registros conforme critérios
    aprovados = []
    inelegiveis = []
    negados = []
    pendentes = []

    for node in all_nodes:
        ts_status = node.get("metas", {}).get("ts_status", None)
        
        if ts_status == "APROVADO":
            aprovados.append(node)
        elif ts_status == "INELEGÍVEL":
            inelegiveis.append(node)
        elif ts_status == "NEGADO":
            negados.append(node)
        elif ts_status == "" or ts_status is None:
            pendentes.append(node)

    # Exibir os resultados da contagem de status
    print("\nContagem por status:")
    for status, count in status_counts.items():
        print(f"{status}: {count}")

    # Atualizar as tags na API
    #update_tag("IMND_MES_ATUAL_REALIZADOS_APROVADOS", len(realizados_aprovados))
    #update_tag("IMND_MES_ATUAL_REALIZADOS_NAO_APROVADOS", len(realizados_nao_aprovados))
    update_tag("IMND_MES_ATUAL_APROVADOS", len(aprovados)) 
    update_tag("IMND_MES_ATUAL_PENDENTES", len(pendentes)) 
    update_tag("IMND_MES_ATUAL_INELEGIVEIS", len(pendentes))
    update_tag("IMND_MES_ATUAL_NEGADOS", len(pendentes))

    print(f"Tarefa executada às {datetime.now()}")

if __name__ == "__main__":
    loadIMNDData()
