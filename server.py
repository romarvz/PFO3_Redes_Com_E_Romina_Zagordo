import socket
import threading
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import pika  # RabbitMQ

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WorkerServer:
    """
    Servidor Worker que recibe tareas por socket y las procesa con un pool de hilos.
    """
    
    def __init__(self, host='0.0.0.0', port=5000, max_workers=4):
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.server_socket = None
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.task_queue = Queue()
        self.running = False
        
        # Estadísticas
        self.tasks_processed = 0
        self.tasks_failed = 0
        
        logger.info(f"Inicializando servidor en {host}:{port} con {max_workers} workers")
    
    def connect_rabbitmq(self):
        """Establece conexión con RabbitMQ para comunicación entre servidores"""
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='localhost')
            )
            channel = connection.channel()
            channel.queue_declare(queue='task_results', durable=True)
            logger.info("Conectado a RabbitMQ")
            return channel
        except Exception as e:
            logger.error(f"Error conectando a RabbitMQ: {e}")
            return None
    
    def start(self):
        """Inicia el servidor y comienza a escuchar conexiones"""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            logger.info(f"✓ Servidor escuchando en {self.host}:{self.port}")
            
            # Inicia thread para procesar tareas
            threading.Thread(target=self.process_tasks, daemon=True).start()
            
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    logger.info(f"Nueva conexión desde {address}")
                    
                    # Maneja cada cliente en un thread separado
                    threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, address),
                        daemon=True
                    ).start()
                    
                except KeyboardInterrupt:
                    logger.info("Servidor detenido por el usuario")
                    break
                except Exception as e:
                    logger.error(f"Error aceptando conexión: {e}")
        
        finally:
            self.shutdown()
    
    def handle_client(self, client_socket, address):
        """Maneja la comunicación con un cliente específico"""
        try:
            while True:
                # Recibe datos del cliente
                data = client_socket.recv(4096)
                
                if not data:
                    logger.info(f"Cliente {address} desconectado")
                    break
                
                # Decodifica la tarea
                try:
                    task_data = json.loads(data.decode('utf-8'))
                    logger.info(f"Tarea recibida de {address}: {task_data}")
                    
                    # Añade información del cliente
                    task_data['client_socket'] = client_socket
                    task_data['client_address'] = address
                    
                    # Encola la tarea para procesamiento
                    self.task_queue.put(task_data)
                    
                    # Envía confirmación inmediata
                    response = {
                        'status': 'accepted',
                        'task_id': task_data.get('task_id', 'unknown'),
                        'message': 'Tarea encolada para procesamiento'
                    }
                    client_socket.send(json.dumps(response).encode('utf-8'))
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Error decodificando JSON: {e}")
                    error_response = {
                        'status': 'error',
                        'message': 'Formato de tarea inválido'
                    }
                    client_socket.send(json.dumps(error_response).encode('utf-8'))
        
        except Exception as e:
            logger.error(f"Error manejando cliente {address}: {e}")
        
        finally:
            client_socket.close()
    
    def process_tasks(self):
        """Procesa tareas de la cola usando el thread pool"""
        logger.info("Iniciado procesador de tareas")
        
        while self.running:
            if not self.task_queue.empty():
                task = self.task_queue.get()
                
                # Envía tarea al pool de hilos
                future = self.thread_pool.submit(self.execute_task, task)
                future.add_done_callback(self.task_completed)
            else:
                time.sleep(0.1)  # Evita uso excesivo de CPU
    
    def execute_task(self, task):
        """Ejecuta una tarea específica"""
        task_id = task.get('task_id', 'unknown')
        task_type = task.get('type', 'unknown')
        
        logger.info(f"Ejecutando tarea {task_id} de tipo {task_type}")
        
        try:
            # Simula procesamiento según tipo de tarea
            if task_type == 'calculate':
                result = self.calculate_task(task)
            elif task_type == 'process_data':
                result = self.process_data_task(task)
            elif task_type == 'store_file':
                result = self.store_file_task(task)
            else:
                result = {'error': f'Tipo de tarea desconocido: {task_type}'}
            
            # Prepara respuesta
            response = {
                'task_id': task_id,
                'status': 'completed',
                'result': result,
                'processed_at': time.time()
            }
            
            self.tasks_processed += 1
            
        except Exception as e:
            logger.error(f"Error ejecutando tarea {task_id}: {e}")
            response = {
                'task_id': task_id,
                'status': 'failed',
                'error': str(e)
            }
            self.tasks_failed += 1
        
        # Envía resultado al cliente
        try:
            client_socket = task['client_socket']
            client_socket.send(json.dumps(response).encode('utf-8'))
        except Exception as e:
            logger.error(f"Error enviando resultado: {e}")
        
        return response
    
    def calculate_task(self, task):
        """Simula una tarea de cálculo intensivo"""
        data = task.get('data', {})
        operation = data.get('operation', 'sum')
        numbers = data.get('numbers', [])
        
        # Simula procesamiento
        time.sleep(2)
        
        if operation == 'sum':
            result = sum(numbers)
        elif operation == 'multiply':
            result = 1
            for num in numbers:
                result *= num
        elif operation == 'average':
            result = sum(numbers) / len(numbers) if numbers else 0
        else:
            result = 0
        
        return {'operation': operation, 'result': result}
    
    def process_data_task(self, task):
        """Simula procesamiento de datos"""
        data = task.get('data', {})
        
        # Simula procesamiento
        time.sleep(1.5)
        
        return {
            'processed_items': len(data),
            'timestamp': time.time()
        }
    
    def store_file_task(self, task):
        """Simula almacenamiento de archivo"""
        filename = task.get('data', {}).get('filename', 'unknown')
        
        # Simula almacenamiento
        time.sleep(1)
        
        return {
            'filename': filename,
            'stored': True,
            'location': f's3://bucket/{filename}'
        }
    
    def task_completed(self, future):
        """Callback cuando una tarea se completa"""
        try:
            result = future.result()
            logger.info(f"Tarea completada: {result.get('task_id')}")
        except Exception as e:
            logger.error(f"Error en tarea: {e}")
    
    def get_stats(self):
        """Retorna estadísticas del servidor"""
        return {
            'tasks_processed': self.tasks_processed,
            'tasks_failed': self.tasks_failed,
            'queue_size': self.task_queue.qsize(),
            'active_workers': self.max_workers
        }
    
    def shutdown(self):
        """Cierra el servidor de forma ordenada"""
        logger.info("Cerrando servidor...")
        self.running = False
        
        if self.server_socket:
            self.server_socket.close()
        
        self.thread_pool.shutdown(wait=True)
        logger.info("Servidor cerrado correctamente")


if __name__ == '__main__':
    # Crea y arranca el servidor
    server = WorkerServer(host='0.0.0.0', port=5000, max_workers=4)
    
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Servidor detenido por el usuario")
    finally:
        stats = server.get_stats()
        logger.info(f"Estadísticas finales: {stats}")