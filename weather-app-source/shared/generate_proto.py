#!/usr/bin/env python3
import subprocess
import os

def generate_proto_files():
    """генерирует Python файлы из .proto"""
    proto_dir = "proto"
    output_dir = "generated"

    # cоздаем директорию для сгенерированных файлов
    os.makedirs(output_dir, exist_ok=True)

    # находим все .proto файлы
    proto_files = [f for f in os.listdir(proto_dir) if f.endswith('.proto')]

    for proto_file in proto_files:
        print(f"Генерируем код для {proto_file}...")
        
        cmd = [
            "python", "-m", "grpc_tools.protoc",
            f"--proto_path={proto_dir}",
            f"--python_out={output_dir}",
            f"--grpc_python_out={output_dir}",
            os.path.join(proto_dir, proto_file)
        ]
        
        try:
            subprocess.run(cmd, check=True)
            print(f"✓ {proto_file} обработан успешно")
        except subprocess.CalledProcessError as e:
            print(f"✗ Ошибка при обработке {proto_file}: {e}")

if __name__ == "__main__":
    generate_proto_files()