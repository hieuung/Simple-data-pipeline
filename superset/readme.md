# Install superset with kubenertes

## Install with helm chart
```
helm repo add superset https://apache.github.io/superset

helm upgrade --install --values my-values.yaml superset superset/superset -n visualization --create-namespace
```