# Start from Miniconda base image
FROM continuumio/miniconda3

RUN apt-get update && apt-get upgrade -y && apt-get clean

WORKDIR /app

# Create a conda env for Python 3.13 (main runtime for FastAPI)
RUN conda create -n py313 python=3.13 -y

# Create a conda env for Python 3.9 (ISA PHM converter runtime)
COPY environment.yml .
RUN conda env create -f environment.yml

# Symlink python3.9 so subprocess calls work
RUN ln -s /opt/conda/envs/isa-phm-converter-env/bin/python3.9 /usr/local/bin/python3.9

# Use py313 as the default shell environment
SHELL ["conda", "run", "-n", "py313", "/bin/bash", "-c"]

# Install FastAPI dependencies in py313
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your app code
COPY . .

# Expose App Runner port
EXPOSE 8080

# Run FastAPI inside py313 with interactive output
CMD ["bash", "-c", "source /opt/conda/bin/activate py313 && uvicorn main:app --host 0.0.0.0 --port 8080 --reload"]

