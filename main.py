import grpc
import csv_maker_pb2
import csv_maker_pb2_grpc
from grpc_reflection.v1alpha import reflection
from concurrent import futures
import logging
import os
import io
import csv



logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



grpc_port = os.environ.get('GRPC_IPPORT') or '0.0.0.0:50051'



class CsvMakerServicer(csv_maker_pb2_grpc.CsvMakerServicer):
   
   def BuildCsv(self, request_iterator, context):
        logger.info(f"BuildCsv request")
        try:
            yield csv_maker_pb2.StreamResponse(content=b'\xef\xbb\xbf')

            for extract_request in request_iterator:
                logger.info(f"Received extract_request: \n {extract_request}")
                output = io.StringIO()
                writer = csv.writer(output, delimiter=';')

                for data_row in extract_request.data_rows:
                    writer.writerow(data_row.row)
                    yield csv_maker_pb2.StreamResponse(content=output.getvalue().encode('utf-8'))
                    output.truncate(0)
                    output.seek(0)
                
        except Exception as e:
            logger.error(f"Error processing BuildCsv request: {e}")
            raise






def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    csv_maker_pb2_grpc.add_CsvMakerServicer_to_server(CsvMakerServicer(), server)
    SERVICE_NAMES = (
            csv_maker_pb2.DESCRIPTOR.services_by_name['CsvMaker'].full_name,
            reflection.SERVICE_NAME,
        )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()








if __name__ == "__main__":
    logger.info(f"Run server on {grpc_port}")
    serve()





"""
# Данные для записи в CSV-файл
data = [
    ['Имя', 'Возраст', 'Город'],
    ['Анна', 25, 'Москва'],
    ['Иван', 30, 'Санкт-Петербург'],
    ['Елена', 35, 'Новосибирск']
]

# Имя CSV-файла для записи
csv_filename = 'data.csv'

# Запись данных в CSV-файл
with open(csv_filename, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter=';')
    for row in data:
        csv_writer.writerow(row)

print(f"CSV файл успешно создан: {csv_filename}")
"""