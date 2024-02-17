# Install Airflow on K8s

## Install helm 
```
    curl https://baltocdn.com/helm/signing.asc | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null
    sudo apt-get install apt-transport-https --yes
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list
    sudo apt-get update
    sudo apt-get install helm
```

## Install Airflow

https://airflow.apache.org/docs/helm-chart/stable/production-guide.html#webserver-secret-key

```
    helm repo add apache-airflow https://airflow.apache.org
    helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace
```

## Upgrade chart with custom values.ymal

```
    helm upgrade -f values.yaml airflow apache-airflow/airflow --namespace airflow
```