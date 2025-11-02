import pika
import json
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class RabbitMQHandler:
    """
    Maneja la comunicación entre servidores usando RabbitMQ.
    Permite enviar y recibir mensajes en colas distribuidas.
    """
    
    def __init__(self, host='localhost', port=5672, username='guest', password='guest'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = None
        self.channel = None
        
    def connect(self):
        """Establece conexión con RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            logger.info(f"✓ Conectado a RabbitMQ en {self.host}:{self.port}")
            return True
            
        except Exception as e:
            logger.error(f"Error conectando a RabbitMQ: {e}")
            return False
    
    def declare_queue(self, queue_name, durable=True):
        """
        Declara una cola en RabbitMQ.
        
        Args:
            queue_name (str): Nombre de la cola
            durable (bool): Si la cola sobrevive a reinicios del servidor
        """
        if not self.channel:
            logger.error("No hay conexión activa con RabbitMQ")
            return False
        
        try:
            self.channel.queue_declare(
                queue=queue_name,
                durable=durable
            )
            logger.info(f"Cola '{queue_name}' declarada")
            return True
            
        except Exception as e:
            logger.error(f"Error declarando cola: {e}")
            return False
    
    def publish_message(self, queue_name, message, priority=0):
        """
        Publica un mensaje en una cola.
        
        Args:
            queue_name (str): Nombre de la cola
            message (dict): Mensaje a enviar
            priority (int): Prioridad del mensaje (0-9)
        """
        if not self.channel:
            logger.error("No hay conexión activa con RabbitMQ")
            return False
        
        try:
            # Convierte mensaje a JSON
            message_body = json.dumps(message)
            
            # Publica mensaje
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Hace el mensaje persistente
                    priority=priority
                )
            )
            
            logger.info(f"Mensaje publicado en '{queue_name}': {message.get('task_id', 'unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Error publicando mensaje: {e}")
            return False
    
    def consume_messages(self, queue_name, callback: Callable, auto_ack=False):
        """
        Consume mensajes de una cola.
        
        Args:
            queue_name (str): Nombre de la cola
            callback (Callable): Función que procesa cada mensaje
            auto_ack (bool): Si se confirma automáticamente la recepción
        """
        if not self.channel:
            logger.error("No hay conexión activa con RabbitMQ")
            return
        
        try:
            # Configura prefetch para distribuir carga equitativamente
            self.channel.basic_qos(prefetch_count=1)
            
            def message_callback(ch, method, properties, body):
                """Wrapper para el callback del usuario"""
                try:
                    # Decodifica mensaje
                    message = json.loads(body.decode('utf-8'))
                    
                    # Llama al callback del usuario
                    callback(message)
                    
                    # Confirma procesamiento si no es auto_ack
                    if not auto_ack:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        
                except Exception as e:
                    logger.error(f"Error procesando mensaje: {e}")
                    # Rechaza mensaje y lo reencola
                    if not auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            # Inicia consumo
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=message_callback,
                auto_ack=auto_ack
            )
            
            logger.info(f"Consumiendo mensajes de '{queue_name}'...")
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("Consumo detenido por el usuario")
            self.stop_consuming()
        except Exception as e:
            logger.error(f"Error consumiendo mensajes: {e}")
    
    def stop_consuming(self):
        """Detiene el consumo de mensajes"""
        if self.channel:
            self.channel.stop_consuming()
            logger.info("Consumo de mensajes detenido")
    
    def get_queue_size(self, queue_name):
        """Obtiene el número de mensajes en una cola"""
        if not self.channel:
            return None
        
        try:
            queue = self.channel.queue_declare(
                queue=queue_name,
                durable=True,
                passive=True  # No crea la cola, solo consulta
            )
            return queue.method.message_count
        except Exception as e:
            logger.error(f"Error obteniendo tamaño de cola: {e}")
            return None
    
    def purge_queue(self, queue_name):
        """Elimina todos los mensajes de una cola"""
        if not self.channel:
            return False
        
        try:
            self.channel.queue_purge(queue=queue_name)
            logger.info(f"Cola '{queue_name}' purgada")
            return True
        except Exception as e:
            logger.error(f"Error purgando cola: {e}")
            return False
    
    def declare_exchange(self, exchange_name, exchange_type='direct'):
        """
        Declara un exchange para routing avanzado.
        
        Args:
            exchange_name (str): Nombre del exchange
            exchange_type (str): Tipo ('direct', 'fanout', 'topic', 'headers')
        """
        if not self.channel:
            return False
        
        try:
            self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=True
            )
            logger.info(f"Exchange '{exchange_name}' declarado")
            return True
        except Exception as e:
            logger.error(f"Error declarando exchange: {e}")
            return False
    
    def bind_queue_to_exchange(self, queue_name, exchange_name, routing_key=''):
        """Vincula una cola a un exchange"""
        if not self.channel:
            return False
        
        try:
            self.channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key
            )
            logger.info(f"Cola '{queue_name}' vinculada a exchange '{exchange_name}'")
            return True
        except Exception as e:
            logger.error(f"Error vinculando cola: {e}")
            return False
    
    def close(self):
        """Cierra la conexión con RabbitMQ"""
        try:
            if self.channel:
                self.channel.close()
            if self.connection:
                self.connection.close()
            logger.info("Conexión con RabbitMQ cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")


# Ejemplo de uso
if __name__ == '__main__':
    # Crea handler
    mq = RabbitMQHandler()
    
    # Conecta
    if mq.connect():
        # Declara cola
        mq.declare_queue('test_queue')
        
        # Publica mensaje
        test_message = {
            'task_id': '12345',
            'type': 'test',
            'data': {'message': 'Hola desde RabbitMQ'}
        }
        mq.publish_message('test_queue', test_message)
        
        # Define callback para consumo
        def process_message(message):
            print(f"Mensaje recibido: {message}")
        
        # Consume mensajes (esto bloqueará el thread)
        # mq.consume_messages('test_queue', process_message)
        
        # Cierra conexión
        mq.close()