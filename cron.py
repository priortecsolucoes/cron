import time
from datetime import datetime, date
import calendar
import requests
import os
from collections import Counter
from datetime import datetime, date, timedelta
import pytz
class IMNDDataLoader:
    def __init__(self):
        self.accessToken = os.getenv('IMND_ACCESS_TOKEN')
        if not self.accessToken:
            raise EnvironmentError("A variável de ambiente 'IMND_ACCESS_TOKEN' não foi encontrada")
        self.motivations = {
            "atendimento recorrente",
            "atendimento sos",
            "atendimento pontual", 
            "alta",
            "emergência do cliente",
            "atendimento interrompido pelo cliente",
            "questão pessoal ou emergência do cliente"
        }
        self.filteredNodes = []
        self.authorizedBillable = []
        self.billableNotAuthorized = []
        self.pendingAuthorizationInArrearsCurrentMonth = []
    def updateTag(self, tagName, intValue): #Envia uma requisicao PUT para atualizar uma tag na API 
        try:
            horario = self.setLastRunTime()
            url = "https://fastapi-production-1598.up.railway.app/update-tag"
            headers = {"Content-Type": "application/json"}
            body = {
                "tag_name": tagName,
                "string_value": horario,
                "int_value": intValue,
                "double_value": 0
            }
            response = requests.put(url, json=body, headers=headers)

            if response.status_code == 200:
                print(f"✅ Tag '{tagName}' atualizada com sucesso! Valor: {intValue}")
            else:
                print(f"❌ Erro ao atualizar a tag '{tagName}'. Código: {response.status_code}, Resposta: {response.text}")
        except requests.RequestException as e:
            print(f"❌ Erro na requisição para atualizar a tag '{tagName}': {e}")
        except Exception as e:
            print(f"❌ Erro inesperado ao atualizar a tag '{tagName}': {e}")
    def updateTagHistoryValue(self, tagName, intValue): #Envia uma requisicao PUT para atualizar uma tag na API 
        try:
            horario = self.setLastRunTime()
            url = "https://fastapi-production-1598.up.railway.app/update-tag"
            headers = {"Content-Type": "application/json"}
            body = {
                "tag_name": tagName,
                "string_value": horario,
                "int_value": 0,
                "double_value": 0
            }
            response = requests.put(url, json=body, headers=headers)

            if response.status_code == 200:
                print(f"✅ Tag '{tagName}' atualizada com sucesso! Valor: {intValue}")
            else:
                print(f"❌ Erro ao atualizar a tag '{tagName}'. Código: {response.status_code}, Resposta: {response.text}")
        except requests.RequestException as e:
            print(f"❌ Erro na requisição para atualizar a tag '{tagName}': {e}")
        except Exception as e:
            print(f"❌ Erro inesperado ao atualizar a tag '{tagName}': {e}")
    def requestWithRetries(self, url, maxRetries=2):#Faz uma requisicao com tentativas em caso de excecao
        attempt = 0
        while attempt <= maxRetries:
            try:
                print(url)
                response = requests.get(url)
                response.raise_for_status()  # Levanta exceção para erros HTTP
                return response
            except requests.HTTPError as e:
                print(f"⚠️ Erro HTTP na tentativa {attempt + 1}: {e.response.status_code} - {e.response.text}")
            except requests.RequestException as e:
                print(f"⚠️ Erro na requisição na tentativa {attempt + 1}: {e}")
            except Exception as e:
                print(f"⚠️ Erro inesperado na tentativa {attempt + 1}: {e}")
            attempt += 1
            if attempt > maxRetries:
                print("❌ Todas as tentativas falharam. Verifique sua conexão ou os detalhes da API.")
                return None
            time.sleep(5)  # Aguarda 5 segundos antes de tentar novamente
    def loadData(self):
        print(f"Iniciando tarefa às {datetime.now()}")
        try:
            # Calculando as datas para a API
            firstDayOfMonth = date.today().replace(day=1)
            lastDay = calendar.monthrange(date.today().year, date.today().month)[1]
            lastDayOfMonth = date.today().replace(day=lastDay)

            # Convertendo para o formato aceito pela API
            dateStart = firstDayOfMonth.strftime("%Y-%m-%d")
            dateEnd = lastDayOfMonth.strftime("%Y-%m-%d")
            # Inicializando variáveis de paginação
            page = 1
            hasMore = True
            allNodes = []

            while hasMore:
                apiUrl = f'http://api.imnd.com.br:3000/api/automation/appointments?authorization={self.accessToken}&page={page}&limit=10000&date_start={dateStart}&date_end={dateEnd}'
                print(f"🔄 Requisitando página {page}...")

                requisicao = self.requestWithRetries(apiUrl)
                if requisicao is None:
                    print("❌ Não foi possível obter dados da API após várias tentativas. Finalizando tarefa.")
                    break
                try:
                    data = requisicao.json()
                    allNodes.extend(data.get("nodes", []))
                    hasMore = data.get("metadata", {}).get("pagination", {}).get("has_more", False)
                    page += 1  # Incrementa para a próxima página
                except ValueError as e:
                    print(f"❌ Erro ao decodificar JSON na página {page}: {e}")
                    break  # Interrompe a execução em caso de erro de decodificação
                except Exception as e:
                    print(f"❌ Erro inesperado ao processar a página {page}: {e}")
                    break
                time.sleep(5)

            try:  # Contagem de status
                statusCounts = Counter(node["status"] for node in allNodes)

                # Filtrar registros conforme critérios
                aprovados = []
                inelegiveis = []
                negados = []
                pendentes = []
                tomorrow_pending = []
                today = date.today()
                
                tomorrow = today + timedelta(days=1)
                todayStr = today.strftime("%d/%m/%Y")
                tomorrowStr = tomorrow.strftime("%d/%m/%Y")

                for node in allNodes:
                    tsStatus = node.get("metas", {}).get("ts_status", None)
                    nodeData = node.get("data")
                    if tsStatus == "APROVADO":
                        aprovados.append(node)
                    elif tsStatus == "INELEGÍVEL":
                        inelegiveis.append(node)
                    elif tsStatus == "NEGADO":
                        negados.append(node)
                    elif tsStatus == "" or tsStatus is None:
                        if nodeData == tomorrowStr:
                            tomorrow_pending.append(node)
                        elif nodeData == todayStr:
                            pendentes.append(node)

                print("\nContagem por status:")
                for status, count in statusCounts.items():
                    print(f"{status}: {count}")
            except KeyError as e:
                print(f"❌ Erro ao acessar dados dos nós: {e}")
            except Exception as e:
                print(f"❌ Erro inesperado ao processar os dados: {e}")

            try:  # Chamar as novas funções
                naoAutorizados = self.processNotBillableQueries(allNodes)
                processadosAutorizados = self.processBillableQueries(allNodes, "aprovado")
                processadosInelegiveis = self.processBillableQueries(allNodes, "inelegível")
                processadosNegados = self.processBillableQueries(allNodes, "negado")
                pendentesAtrasadosMesAtual = self.checkPendingAuthorizationForCurrentMonth(allNodes)
                lastUpdate = self.setLastRunTime()
                self.updateTagHistoryValue("IMND_DATA_DA_ULTIMA_EXECUCAO", len(lastUpdate))
                
                # Atualizar as tags na API
                self.updateTag("IMND_MES_ATUAL_APROVADOS", len(aprovados))
                self.updateTag("IMND_MES_ATUAL_PENDENTES", len(pendentes))
                self.updateTag("IMND_MES_ATUAL_PENDENCIAS_IMEDIATAS", len(tomorrow_pending))
                self.updateTag("IMND_MES_ATUAL_INELEGIVEIS", len(inelegiveis))
                self.updateTag("IMND_MES_ATUAL_NEGADOS", len(negados))

                # Atualizar com novos dados processados
                self.updateTag("IMND_MES_ATUAL_FATURAVEIS_NAO_AUTORIZADAS", len(naoAutorizados))
                self.updateTag("IMND_MES_ATUAL_FATURAVEIS_AUTORIZADAS", len(processadosAutorizados))
                self.updateTag("IMND_MES_ATUAL_FATURAVEIS_INELEGIVEIS", len(processadosInelegiveis))
                self.updateTag("IMND_MES_ATUAL_FATURAVEIS_NEGADOS", len(processadosNegados))
                self.updateTag("IMND_AUTORIZACAO_PENDENTES_ATRASADOS_MES_ATUAL", len(pendentesAtrasadosMesAtual))
            except Exception as e:
                print(f"❌ Erro ao atualizar tags ou processar dados: {e}")
        except Exception as e:
            print(f"❌ Erro geral na execução da tarefa: {e}")
        print(f"Tarefa executada às {datetime.now()}")
        
    def checkPendingAuthorizationForCurrentMonth(self, nodes):
        today = date.today()
        for node in nodes:
            try:
                nodeDateTimeStr = node.get("data", "01/01/1970")
                nodeDateTime = datetime.strptime(nodeDateTimeStr, "%d/%m/%Y").date()
                nodeMotivation = (node.get("motivacao") or "").lower().strip()
                nodeStatus = node.get("metas", {}).get("ts_status")

                # Verifica se a data está entre até 3 dias antes da data atual
                if (nodeMotivation is None or nodeMotivation == "" or nodeMotivation in self.motivations) and (nodeStatus is None or nodeStatus == ""):
                    print(f"Consulta faturável não autorizada encontrada em {node['data']}")
                    self.pendingAuthorizationInArrearsCurrentMonth.append({
                        "data": node["data"],
                        "ts_status": node.get("metas", {}).get("ts_status", ""),
                    })
            except ValueError as erro:
                print(f"❌ Erro ao converter data '{node.get('data', 'Desconhecida')}': {erro}")
        return self.pendingAuthorizationInArrearsCurrentMonth
        
    def processNotBillableQueries(self, nodes):
        today = date.today()
        limitDate = today - timedelta(days=3)  # Data limite: 3 dias antes de hoje
        startOfMonth = today.replace(day=1)    # Início do mês atual

        for node in nodes:  # Filtra os dados e cria o filteredNodes com a estrutura desejada
            try:
                nodeDateTimeStr = node.get("data", "01/01/1970")
                nodeDateTime = datetime.strptime(nodeDateTimeStr, "%d/%m/%Y").date()
                nodeMotivation = (node.get("motivacao") or "").lower().strip()
                nodeStatus = node.get("metas", {}).get("ts_status")

                if startOfMonth <= nodeDateTime < limitDate and (nodeMotivation is None or nodeMotivation == "" or nodeMotivation in self.motivations) and (nodeStatus is None or nodeStatus == ""):# Verifica se a data está entre o início do mês e registros de mais de 3 dias atrás
                    print(f"Consulta faturável não autorizada encontrada em {node['data']}")
                    self.billableNotAuthorized.append({
                        "data": node["data"],
                        "motivacao": node["motivacao"],
                        "ts_status": node.get("metas", {}).get("ts_status", ""),
                    })
            except ValueError as erro:
                print(f"❌ Erro ao converter data '{node.get('data', 'Desconhecida')}': {erro}")
        print('Consultas Faturaveis nao autorizadas', self.billableNotAuthorized)
        return self.billableNotAuthorized

    def processBillableQueries(self, nodes, status):
        today = date.today()
        limitDate = today - timedelta(days=3)
        result = []  
        for node in nodes:
            try:
                nodeDateTimeStr = node.get("data", "01/01/1970")
                nodeDateTime = datetime.strptime(nodeDateTimeStr, "%d/%m/%Y").date()
                nodeMotivation = (node.get("motivacao") or "").lower().strip()
                nodeStatus = (node.get("metas", {}).get("ts_status") or "").lower().strip()

                if (nodeDateTime <= limitDate and (nodeMotivation is None or nodeMotivation == "" or nodeMotivation in self.motivations) and nodeStatus == status):
                    result.append({
                        "data": node["data"],
                        "motivacao": node["motivacao"]
                    })
            except ValueError as erro:
                print(f"❌ Erro ao converter data '{node.get('data', 'Desconhecida')}': {erro}")
        return result
    def setLastRunTime(self):
        timeZone = pytz.timezone('America/Sao_Paulo') #Definindo o fuso horario de brasilia, nao esta errado, realmente se orienta por SP
        dateTimeBrasilia = datetime.now(timeZone)
        updatedDateandTime =dateTimeBrasilia.strftime('%d/%m/%Y %H:%M:%S')
        return updatedDateandTime
if __name__ == "__main__":
    try:
        loader = IMNDDataLoader()
        loader.loadData()
    except Exception as e:
        print(f"❌ Erro fatal: {e}")
