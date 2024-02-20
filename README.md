# Simple-data-pipeline
Simple data-pipeline, deployment using containerization with Kubenetes and Helm for ETL learning

## Tech stack
- Containerization (docker, kubernetes, minikube, helm)
- Airflow
- SQL (Postgres)

## Descriptions
This pipeline extract data from production database (from project [Simple web-app](https://github.com/hieuung/Simple-web-app)) transform and load into a data sink using python, SQL(Postgres) and Airflow for job schedualing.

## Deployment
- Install [docker](https://docs.docker.com/engine/install/ubuntu/), [kubernetes](https://kubernetes.io/docs/tasks/tools/), [minikube](https://minikube.sigs.k8s.io/docs/start/), and [helm](https://helm.sh/docs/intro/install/)

- Clone and build project [Simple web-app](https://github.com/hieuung/Simple-web-app) following instruction.

- Clone this project to local.

- (Optional) Build your own Airflow image (build your own dags) using .Dockerfile provided

- Deploy Airflow on minikube using built Airflow image (currently my Airflow image)
```
cd `path_to_this_repo`/hieu_airflow/deployment

helm repo add apache-airflow https://airflow.apache.org
    helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace

helm upgrade -f values.yaml airflow apache-airflow/airflow --namespace airflow
```

## Result
- Verify deployment, service
```
kubectl get depolyment -n airflow
```

```
kubectl get service -n airflow
```

> **_NOTE:_**  Using ```minkibe tunnel``` if LoadBalancer not exposes External-IP