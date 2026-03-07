import argparse
import os
import subprocess
import sys
from dotenv import load_dotenv


def cargar_entorno(env_name):
    env_file = f".env.{env_name}"
    if os.path.exists(env_file):
        load_dotenv(env_file, override=True)
        print(f"Entorno cargado desde {env_file}")
    elif os.path.exists(".env"):
        load_dotenv(".env", override=True)
        print(f"No existe {env_file}. Usando .env")
    else:
        print(f"No se encontro {env_file} ni .env. Se usaran variables del sistema.")


def main():
    parser = argparse.ArgumentParser(description="Ejecuta el pipeline con entorno configurable")
    parser.add_argument("--env", default="local", help="Nombre de entorno (ejemplo: local, dev, prod)")
    args = parser.parse_args()

    cargar_entorno(args.env)

    command = [sys.executable, os.path.join("pipeline", "run_pipeline.py")]
    result = subprocess.run(command)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
