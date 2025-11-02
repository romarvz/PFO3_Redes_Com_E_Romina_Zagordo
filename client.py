import socket
import json
import uuid
import time
import logging
import threading

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TaskClient:
    """
    Cliente que se conecta al servidor y envía tareas para procesar.
    """
    
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.socket = None
        self.connected = False
        self.pending_tasks = {}
        
    def connect(self):
        """Establece conexión con el servidor"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.connected = True
            logger.info(f"✓ Conectado al servidor {self.host}:{self.port}")
            
            # Inicia thread para escuchar respuestas
            threading.Thread(target=self.listen_responses, daemon=True).start()
            
            return True
        except Exception as e:
            logger.error(f"Error conectando al servidor: {e}")
            self.connected = False
            return False
    
    def disconnect(self):
        """Cierra la conexión con el servidor"""
        if self.socket:
            self.socket.close()
            self.connected = False
            logger.info("Desconectado del servidor")
    
    def send_task(self, task_type, data):
        """
        Envía una tarea al servidor.
        
        Args:
            task_type (str): Tipo de tarea ('calculate', 'process_data', 'store_file')
            data (dict): Datos de la tarea
            
        Returns:
            str: ID de la tarea enviada
        """
        if not self.connected:
            logger.error("No hay conexión con el servidor")
            return None
        
        # Genera ID único para la tarea
        task_id = str(uuid.uuid4())
        
        # Prepara la tarea
        task = {
            'task_id': task_id,
            'type': task_type,
            'data': data,
            'timestamp': time.time()
        }
        
        try:
            # Envía la tarea
            message = json.dumps(task).encode('utf-8')
            self.socket.send(message)
            
            # Registra tarea pendiente
            self.pending_tasks[task_id] = {
                'type': task_type,
                'sent_at': time.time(),
                'status': 'pending'
            }
            
            logger.info(f"Tarea enviada: {task_id} - Tipo: {task_type}")
            return task_id
            
        except Exception as e:
            logger.error(f"Error enviando tarea: {e}")
            return None
    
    def listen_responses(self):
        """Escucha respuestas del servidor en un thread separado"""
        logger.info("Escuchando respuestas del servidor...")
        
        while self.connected:
            try:
                data = self.socket.recv(4096)
                
                if not data:
                    logger.warning("Servidor cerró la conexión")
                    self.connected = False
                    break
                
                # Procesa respuesta
                response = json.loads(data.decode('utf-8'))
                self.handle_response(response)
                
            except Exception as e:
                if self.connected:
                    logger.error(f"Error recibiendo respuesta: {e}")
                break
    
    def handle_response(self, response):
        """Procesa una respuesta del servidor"""
        task_id = response.get('task_id')
        status = response.get('status')
        
        if status == 'accepted':
            logger.info(f"✓ Tarea {task_id} aceptada por el servidor")
            
        elif status == 'completed':
            result = response.get('result')
            logger.info(f"✓ Tarea {task_id} completada")
            logger.info(f"  Resultado: {result}")
            
            # Actualiza estado
            if task_id in self.pending_tasks:
                self.pending_tasks[task_id]['status'] = 'completed'
                self.pending_tasks[task_id]['result'] = result
                
        elif status == 'failed':
            error = response.get('error')
            logger.error(f"✗ Tarea {task_id} falló: {error}")
            
            if task_id in self.pending_tasks:
                self.pending_tasks[task_id]['status'] = 'failed'
                self.pending_tasks[task_id]['error'] = error
        
        elif status == 'error':
            message = response.get('message')
            logger.error(f"Error del servidor: {message}")
    
    def send_calculation(self, operation, numbers):
        """
        Envía una tarea de cálculo.
        
        Args:
            operation (str): 'sum', 'multiply', 'average'
            numbers (list): Lista de números
        """
        return self.send_task('calculate', {
            'operation': operation,
            'numbers': numbers
        })
    
    def send_data_processing(self, items):
        """Envía una tarea de procesamiento de datos"""
        return self.send_task('process_data', items)
    
    def send_file_storage(self, filename, content):
        """Envía una tarea de almacenamiento de archivo"""
        return self.send_task('store_file', {
            'filename': filename,
            'content': content
        })
    
    def get_task_status(self, task_id):
        """Obtiene el estado de una tarea"""
        return self.pending_tasks.get(task_id, {'status': 'unknown'})
    
    def wait_for_task(self, task_id, timeout=30):
        """
        Espera a que una tarea se complete.
        
        Args:
            task_id (str): ID de la tarea
            timeout (int): Tiempo máximo de espera en segundos
            
        Returns:
            dict: Resultado de la tarea o None si timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            task_info = self.get_task_status(task_id)
            
            if task_info['status'] == 'completed':
                return task_info.get('result')
            elif task_info['status'] == 'failed':
                return {'error': task_info.get('error')}
            
            time.sleep(0.5)
        
        logger.warning(f"Timeout esperando tarea {task_id}")
        return None
    
    def get_statistics(self):
        """Retorna estadísticas del cliente"""
        total = len(self.pending_tasks)
        completed = sum(1 for t in self.pending_tasks.values() if t['status'] == 'completed')
        failed = sum(1 for t in self.pending_tasks.values() if t['status'] == 'failed')
        pending = sum(1 for t in self.pending_tasks.values() if t['status'] == 'pending')
        
        return {
            'total_tasks': total,
            'completed': completed,
            'failed': failed,
            'pending': pending
        }


def interactive_menu():
    """Menú interactivo para el cliente"""
    client = TaskClient()
    
    print("=" * 50)
    print("CLIENTE DE SISTEMA DISTRIBUIDO")
    print("=" * 50)
    
    if not client.connect():
        print("No se pudo conectar al servidor. Verifica que esté corriendo.")
        return
    
    while True:
        print("\n" + "=" * 50)
        print("OPCIONES:")
        print("1. Enviar cálculo (suma)")
        print("2. Enviar cálculo (multiplicación)")
        print("3. Enviar cálculo (promedio)")
        print("4. Procesar datos")
        print("5. Almacenar archivo")
        print("6. Ver estadísticas")
        print("7. Enviar múltiples tareas (prueba de carga)")
        print("0. Salir")
        print("=" * 50)
        
        choice = input("\nSelecciona una opción: ").strip()
        
        if choice == '1':
            numbers = [10, 20, 30, 40, 50]
            task_id = client.send_calculation('sum', numbers)
            print(f"Tarea de suma enviada: {task_id}")
            print(f"Números: {numbers}")
            
        elif choice == '2':
            numbers = [2, 3, 4, 5]
            task_id = client.send_calculation('multiply', numbers)
            print(f"Tarea de multiplicación enviada: {task_id}")
            print(f"Números: {numbers}")
            
        elif choice == '3':
            numbers = [100, 200, 300, 400, 500]
            task_id = client.send_calculation('average', numbers)
            print(f"Tarea de promedio enviada: {task_id}")
            print(f"Números: {numbers}")
            
        elif choice == '4':
            data = {'items': list(range(1000)), 'filter': 'even'}
            task_id = client.send_data_processing(data)
            print(f"Tarea de procesamiento enviada: {task_id}")
            
        elif choice == '5':
            filename = f"archivo_{int(time.time())}.txt"
            content = "Contenido de ejemplo del archivo"
            task_id = client.send_file_storage(filename, content)
            print(f"Tarea de almacenamiento enviada: {task_id}")
            print(f"Archivo: {filename}")
            
        elif choice == '6':
            stats = client.get_statistics()
            print("\nESTADÍSTICAS DEL CLIENTE:")
            print(f"  Total de tareas: {stats['total_tasks']}")
            print(f"  Completadas: {stats['completed']}")
            print(f"  Fallidas: {stats['failed']}")
            print(f"  Pendientes: {stats['pending']}")
            
        elif choice == '7':
            print("\n¿Cuántas tareas deseas enviar?")
            num_tasks = int(input("Número: "))
            
            print(f"\nEnviando {num_tasks} tareas...")
            start = time.time()
            
            for i in range(num_tasks):
                client.send_calculation('sum', [i, i+1, i+2])
            
            elapsed = time.time() - start
            print(f"✓ {num_tasks} tareas enviadas en {elapsed:.2f} segundos")
            print(f"  Tasa: {num_tasks/elapsed:.2f} tareas/seg")
            
        elif choice == '0':
            print("\nCerrando cliente...")
            client.disconnect()
            break
        
        else:
            print("Opción inválida")
        
        time.sleep(1)


if __name__ == '__main__':
    try:
        interactive_menu()
    except KeyboardInterrupt:
        print("\n\nCliente detenido por el usuario")