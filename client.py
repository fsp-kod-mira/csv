import grpc
import csv
import io
import csv_maker_pb2
import csv_maker_pb2_grpc


def generate_test_data():
    extract_request = csv_maker_pb2.ExtractRequest()
    data_rows = [
        ["Name", "Age", "City"],
        ["Anna", "25", "Москва"],
        ["Ivan", "30", "Санкт-Петербург"],
        ["Elena", "35", "Новосибирск"]
    ]
    for row in data_rows:
        data_row = csv_maker_pb2.DataRow(row=row)
        extract_request.data_rows.append(data_row)
    return extract_request


def run_client():
    try:
        channel = grpc.insecure_channel('localhost:50051')
        client = csv_maker_pb2_grpc.CsvMakerStub(channel)
        
        request = generate_test_data()
        
        response_iterator = client.BuildCsv(iter([request]))
        
        with open("file.csv", "wb") as file:
            for response in response_iterator:
                file.write(response.content)
        
    except grpc.RpcError as e:
        print(f"gRPC Error: {e}")


if __name__ == "__main__":
    run_client()
