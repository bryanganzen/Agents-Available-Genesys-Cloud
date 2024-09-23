import requests
import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException
from datetime import datetime, timedelta
import uuid
import time
import csv
import io
import pandas as pd

token_url = 'Token de acceso de Genesys Cloud'

def obtener_token_de_acceso():
    response = requests.get(token_url)
    if response.status_code == 200:
        access_token = response.json().get('token')
        return access_token
    else:
        print(f"Error al obtener el token de acceso: {response.status_code}")
        return None

def obtener_nombre_agente(user_id, access_token):
    PureCloudPlatformClientV2.configuration.access_token = access_token
    api_instance = PureCloudPlatformClientV2.UsersApi()

    try:
        user = api_instance.get_user(user_id)
        return user.name
    except ApiException as e:
        print(f"Exception when calling UsersApi->get_user: {e}")
        return "Nombre no encontrado"

def obtener_detalles_conversacion(conversation_id, access_token):
    PureCloudPlatformClientV2.configuration.access_token = access_token
    api_instance = PureCloudPlatformClientV2.ConversationsApi()

    try:
        api_response = api_instance.get_analytics_conversation_details(conversation_id)

        queue_id = None
        agent_id = None
        queue_entry_time = None
        wait_time_in_queue = None
        queue_name = None

        for participant in api_response.participants:
            if participant.purpose == "agent" and agent_id is None:
                agent_id = participant.user_id
            if participant.purpose == "acd" and queue_name is None:
                queue_name = participant.participant_name

            for session in participant.sessions:
                for segment in session.segments:
                    if hasattr(segment, 'queue_id') and queue_id is None:
                        queue_id = segment.queue_id

                    if segment.segment_type == "interact" and queue_entry_time is None and participant.purpose == "customer":
                        queue_entry_time = segment.segment_start

                if queue_entry_time and agent_id and wait_time_in_queue is None:
                    for metric in session.metrics:
                        if metric.name == "tAnswered":
                            wait_time_in_queue = metric.value

            if queue_id and agent_id and queue_entry_time and wait_time_in_queue and queue_name:
                break

        return queue_id, queue_entry_time, wait_time_in_queue, agent_id, queue_name

    except ApiException as e:
        print(f"Exception when calling ConversationsApi->get_analytics_conversation_details: {e}")
        return None, None, None, None, None

def crear_informe(queue_id, queue_entry_time, access_token):
    PureCloudPlatformClientV2.configuration.access_token = access_token
    api_instance = PureCloudPlatformClientV2.AnalyticsApi()

    nombre_informe = f"Exportación de Miembros de Cola_{uuid.uuid4()}"

    fecha_inicio = queue_entry_time
    fecha_final = fecha_inicio + timedelta(minutes=10)
    intervalo = f"{fecha_inicio.strftime('%Y-%m-%dT%H:%M:%SZ')}/{fecha_final.strftime('%Y-%m-%dT%H:%M:%SZ')}"

    body = {
        "name": nombre_informe,
        "timeZone": "America/Mexico_City",
        "exportFormat": "CSV",
        "interval": intervalo,
        "period": "PT30M",
        "viewType": "QUEUE_AGENT_DETAIL_VIEW",
        "filter": {
            "queueIds": [queue_id],
            "mediaTypes": ["callback", "voice"],
            "filterUsersByQueueIds": [queue_id],
            "userState": "ActiveAndInactive"
        },
        "read": True,
        "locale": "es",
        "csvDelimiter": "COMMA",
        "intervalKeyType": "ConversationStart",
        "includeDurationFormatInHeader": True,
        "durationFormat": "Milliseconds",
        "exportAllowedToRerun": False,
        "enabled": False
    }

    try:
        api_response = api_instance.post_analytics_reporting_exports(body)
        return nombre_informe
    except ApiException as e:
        print(f"Exception when calling AnalyticsApi->post_analytics_reporting_exports: {e}")
        return None

def obtener_url_descarga_informe(nombre_informe, access_token):
    PureCloudPlatformClientV2.configuration.access_token = access_token
    api_instance = PureCloudPlatformClientV2.AnalyticsApi()

    try:
        while True:
            api_response = api_instance.get_analytics_reporting_exports(page_number=1, page_size=25)

            for informe in api_response.entities:
                if informe.name == nombre_informe:
                    if informe.status == "COMPLETED" and informe.download_url:
                        return informe.download_url
                    else:
                        print("El informe aún no está listo para ser descargado. Esperando 10 segundos...")
            time.sleep(10)

    except ApiException as e:
        print(f"Exception when calling AnalyticsApi->get_analytics_reporting_exports: {e}")
        return None

def obtener_ids_agentes_desde_csv(url, access_token):
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        csv_file = io.StringIO(response.text)
        csv_reader = csv.reader(csv_file)

        headers = next(csv_reader)
        try:
            indice_id_agente = headers.index("ID del agente")
        except ValueError:
            print("No se encontró la columna 'ID del agente' en el CSV.")
            return []

        agent_ids = set()
        for row in csv_reader:
            if row[indice_id_agente]:
                agent_ids.add(row[indice_id_agente])
        
        return list(agent_ids)
    else:
        print(f"Error al descargar el archivo: {response.status_code} - {response.text}")
        return []

def crear_informe_por_agente(agent_ids, access_token, intervalo):
    PureCloudPlatformClientV2.configuration.access_token = access_token
    api_instance = PureCloudPlatformClientV2.AnalyticsApi()

    nombre_informe = f"AGENTES_CONECTADOS_HORARIO_LLAMADA_{uuid.uuid4()}"

    body = {
        "name": nombre_informe,
        "timeZone": "America/Mexico_City",
        "exportFormat": "CSV",
        "interval": intervalo,
        "period": "PT30M",
        "viewType": "AGENT_TIMELINE_SUMMARY_VIEW",
        "filter": {
            "userIds": agent_ids
        },
        "read": True,
        "locale": "es",
        "hasFormatDurations": True,
        "hasSplitFilters": False,
        "excludeEmptyRows": False,
        "hasSplitByMedia": False,
        "hasSummaryRow": False,
        "csvDelimiter": "COMMA",
        "hasCustomParticipantAttributes": False,
        "intervalKeyType": "ConversationStart",
        "includeDurationFormatInHeader": False,
        "durationFormat": "Milliseconds",
        "exportAllowedToRerun": False,
        "enabled": False
    }

    try:
        api_response = api_instance.post_analytics_reporting_exports(body)
        return nombre_informe
    except ApiException as e:
        print(f"Exception when calling AnalyticsApi->post_analytics_reporting_exports: {e}")
        return None

def crear_informe_interaccion_filtrado(agent_ids, access_token, intervalo):
    PureCloudPlatformClientV2.configuration.access_token = access_token
    api_instance = PureCloudPlatformClientV2.AnalyticsApi()

    nombre_informe = f"INTERACCION_FILTRADA_{uuid.uuid4()}"

    body = {
        "name": nombre_informe,
        "timeZone": "America/Mexico_City",
        "exportFormat": "CSV",
        "interval": intervalo,
        "period": "PT30M",
        "viewType": "INTERACTION_SEARCH_VIEW",
        "filter": {
            "userIds": agent_ids
        },
        "read": True,
        "locale": "es",
        "hasFormatDurations": True,
        "hasSplitFilters": False,
        "excludeEmptyRows": False,
        "hasSplitByMedia": False,
        "hasSummaryRow": False,
        "csvDelimiter": "COMMA",
        "hasCustomParticipantAttributes": False,
        "intervalKeyType": "ConversationStart",
        "includeDurationFormatInHeader": False,
        "durationFormat": "Milliseconds",
        "exportAllowedToRerun": False,
        "enabled": False
    }

    try:
        api_response = api_instance.post_analytics_reporting_exports(body)
        return nombre_informe
    except ApiException as e:
        print(f"Exception when calling AnalyticsApi->post_analytics_reporting_exports: {e}")
        return None

def convertir_a_segundos(tiempo):
    if not tiempo or tiempo.strip() == '':
        return 0
    try:
        partes = tiempo.split(':')
        horas = int(partes[0])
        minutos = int(partes[1])
        segundos = float(partes[2])
        return horas * 3600 + minutos * 60 + segundos
    except (ValueError, IndexError):
        print(f"Error al convertir tiempo: {tiempo}. Estableciendo a 0.")
        return 0

def procesar_informe_desde_csv(url, access_token, intervalo_inicio, intervalo_fin):
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        csv_file = io.StringIO(response.text)
        csv_reader = csv.DictReader(csv_file)

        filtered_agent_ids = []
        primer_usuarios_set = set()
        total_registros = 0
        datos_interaccion = []
        ids_usuarios_en_cola = []
        estados_conexion = []
        interacciones_agentes_conectados = []

        intervalo_inicio_ajustado = (intervalo_inicio - timedelta(hours=6)).replace(tzinfo=None)
        intervalo_fin_ajustado = (intervalo_fin - timedelta(hours=6)).replace(tzinfo=None)

        for row in csv_reader:
            nombre_agente = row.get("Nombre del agente", "N/A")
            id_agente = row.get("ID del agente", "N/A")
            hora_inicio = row.get("Hora de inicio", "N/A")
            hora_finalizacion = row.get("Hora de finalizaciÃ³n", "N/A")
            estado_secundario = row.get("Estado secundario", "N/A")
            duracion = row.get("DuraciÃ³n", "N/A")
            
            estados_conexion.append({
                "Nombre del agente": nombre_agente,
                "ID del agente": id_agente,
                "Hora de inicio": hora_inicio,
                "Hora de finalización": hora_finalizacion,
                "Estado secundario": estado_secundario,
                "Duración": duracion
            })
            
            print(f"Nombre del agente: {nombre_agente}")
            print(f"ID del agente: {id_agente}")
            print(f"Hora de inicio: {hora_inicio}")
            print(f"Hora de finalización: {hora_finalizacion}")
            print(f"Estado secundario: {estado_secundario}")
            print(f"Duración: {duracion}")
            print("-" * 40)
            filtered_agent_ids.append(id_agente)

            usuarios = row.get("Usuarios", "N/A")
            primer_usuario = usuarios.split(";")[0] if usuarios != "N/A" else "N/A"

            if primer_usuario != "N/A":
                primer_usuarios_set.add(primer_usuario)

            fecha = row.get("Fecha", "N/A")
            fecha_datetime = None
            if fecha != "N/A":
                try:
                    fecha_datetime = datetime.strptime(fecha, "%d/%m/%y %H:%M:%S").replace(tzinfo=None)
                except ValueError:
                    try:
                        fecha_datetime = datetime.strptime(fecha, "%d/%m/%y %H:%M").replace(tzinfo=None)
                    except ValueError:
                        print(f"Formato de fecha no reconocido: {fecha}")
                        continue

            if fecha_datetime and intervalo_inicio_ajustado <= fecha_datetime <= intervalo_fin_ajustado:
                direccion = row.get("DirecciÃ³n", "N/A")
                cola = row.get("Cola", "N/A")
                dnis = row.get("DNIS", "N/A")
                
                conversacion_total_str = row.get("ConversaciÃ³n total", "00:00:00.000")
                total_acw_str = row.get("Total de ACW", "00:00:00.000")
                
                conversacion_total = convertir_a_segundos(conversacion_total_str)
                total_acw = convertir_a_segundos(total_acw_str)
                manejo_total = conversacion_total + total_acw

                interacciones_agentes_conectados.append({
                    "Primer Usuario": primer_usuario,
                    "Fecha Ajustada": fecha_datetime,
                    "Dirección": direccion,
                    "Cola": cola,
                    "Conversación Total": conversacion_total,
                    "Total de ACW": total_acw,
                    "Manejo Total": manejo_total,
                    "DNIS": dnis
                })

                print(f"Primer Usuario: {primer_usuario}")
                print(f"Fecha Ajustada: {fecha_datetime}")
                print(f"Dirección: {direccion}")
                print(f"Cola: {cola}")
                print(f"Conversación Total: {conversacion_total} segundos")
                print(f"Total de ACW: {total_acw} segundos")
                print(f"Manejo Total: {manejo_total} segundos")
                print(f"DNIS: {dnis}")
                print("-" * 40)

                total_registros += 1

        print(f"Total de registros procesados: {total_registros}")
        print(f"Total de primeros usuarios únicos: {len(primer_usuarios_set)}")

        return filtered_agent_ids, datos_interaccion, ids_usuarios_en_cola, estados_conexion, interacciones_agentes_conectados
    else:
        print(f"Error al descargar el archivo: {response.status_code} - {response.text}")
        return [], [], [], [], []

def guardar_en_excel(datos_interaccion, ids_usuarios_en_cola, estados_conexion, interacciones_agentes_conectados):
    with pd.ExcelWriter('reporte_interacciones.xlsx', engine='openpyxl') as writer:
        if datos_interaccion:
            df_interaccion = pd.DataFrame(datos_interaccion)
            df_interaccion['Queue Entry Time'] = df_interaccion['Queue Entry Time'].apply(
                lambda x: (x - timedelta(hours=6)).replace(tzinfo=None) if isinstance(x, datetime) else x
            )
            df_interaccion.to_excel(writer, sheet_name='Interaccion', index=False)

        if ids_usuarios_en_cola:
            df_usuarios = pd.DataFrame(ids_usuarios_en_cola, columns=["ID de usuario", "Nombre de usuario"])
            df_usuarios.to_excel(writer, sheet_name='Usuarios', index=False)

        if estados_conexion:
            df_estados = pd.DataFrame(estados_conexion)
            df_estados.to_excel(writer, sheet_name='Estados', index=False)

        if interacciones_agentes_conectados:
            df_interacciones = pd.DataFrame(interacciones_agentes_conectados)
            df_interacciones['Fecha Ajustada'] = df_interacciones['Fecha Ajustada'].apply(
                lambda x: x.replace(tzinfo=None) if isinstance(x, datetime) else x
            )
            df_interacciones.to_excel(writer, sheet_name='Interacciones', index=False)

def main():
    access_token = obtener_token_de_acceso()
    if access_token is None:
        print("No se pudo obtener el token de acceso. Abortando.")
        return

    conversation_id = input("Por favor, ingrese el ID de la conversación: ").strip()
    queue_id, queue_entry_time, wait_time_in_queue, agent_id, queue_name = obtener_detalles_conversacion(conversation_id, access_token)

    if queue_id and queue_entry_time:
        print(f"Queue ID: {queue_id}")
        print(f"Queue Name: {queue_name}")
        print(f"Queue Entry Time: {queue_entry_time}")
        print(f"Wait Time in Queue: {wait_time_in_queue}")
        print(f"Agent ID: {agent_id}")

        datos_interaccion = [{
            "ID de la conversación": conversation_id,
            "Queue ID": queue_id,
            "Queue Name": queue_name,
            "Queue Entry Time": queue_entry_time,
            "Wait Time in Queue": wait_time_in_queue,
            "Agent ID": agent_id
        }]

        nombre_informe = crear_informe(queue_id, queue_entry_time, access_token)
        if nombre_informe:
            print(f"Informe creado: {nombre_informe}")
            time.sleep(10)

            url_descarga = obtener_url_descarga_informe(nombre_informe, access_token)
            if url_descarga:
                print(f"URL de descarga del informe: {url_descarga}")
                
                agent_ids = obtener_ids_agentes_desde_csv(url_descarga, access_token)
                print(f"IDs de agentes: {agent_ids}")

                ids_usuarios_en_cola = [{"ID de usuario": id_usuario, "Nombre de usuario": obtener_nombre_agente(id_usuario, access_token)} for id_usuario in agent_ids]

                intervalo = f"{queue_entry_time.strftime('%Y-%m-%dT%H:%M:%SZ')}/{(queue_entry_time + timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%SZ')}"
                intervalo_inicio = queue_entry_time
                intervalo_fin = queue_entry_time + timedelta(minutes=10)

                nombre_informe_agentes = crear_informe_por_agente(agent_ids, access_token, intervalo)
                if nombre_informe_agentes:
                    print(f"Informe por agente creado: {nombre_informe_agentes}")
                    time.sleep(10)

                    url_descarga_agente = obtener_url_descarga_informe(nombre_informe_agentes, access_token)
                    if url_descarga_agente:
                        print(f"URL de descarga del informe por agente: {url_descarga_agente}")
                        filtered_agent_ids, _, _, estados_conexion, interacciones_agentes_conectados = procesar_informe_desde_csv(url_descarga_agente, access_token, intervalo_inicio, intervalo_fin)

                        if filtered_agent_ids:
                            nombre_informe_filtrado = crear_informe_interaccion_filtrado(filtered_agent_ids, access_token, intervalo)
                            if nombre_informe_filtrado:
                                print(f"Informe de interacción filtrada creado: {nombre_informe_filtrado}")
                                time.sleep(10)
                                url_descarga_filtrado = obtener_url_descarga_informe(nombre_informe_filtrado, access_token)
                                if url_descarga_filtrado:
                                    print(f"URL de descarga del informe filtrado: {url_descarga_filtrado}")
                                    _, _, _, _, interacciones_agentes_conectados = procesar_informe_desde_csv(url_descarga_filtrado, access_token, intervalo_inicio, intervalo_fin)

                                    guardar_en_excel(datos_interaccion, ids_usuarios_en_cola, estados_conexion, interacciones_agentes_conectados)
                                else:
                                    print("No se pudo obtener la URL de descarga del informe filtrado.")
                            else:
                                print("No se pudo crear el informe de interacción filtrada.")
                        else:
                            print("No se encontraron agentes con los estados secundarios especificados.")
                    else:
                        print("No se pudo obtener la URL de descarga del informe por agente.")
                else:
                    print("No se pudo crear el informe por agente.")
            else:
                print("No se pudo obtener la URL de descarga del informe inicial.")
        else:
            print("No se pudo crear el informe inicial.")
    else:
        print("No se pudo obtener la información necesaria de la conversación.")

if __name__ == "__main__":
    main()