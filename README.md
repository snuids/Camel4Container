# Camel4Container

Un conteneur qui exécute Apache Camel 4 : le jar embarque le chargeur et les dépendances, et il lit
un **`camel-context.xml` fourni de l'extérieur** (monté par le compose). Les routes ne sont donc pas
dans l'image : c'est le fichier monté qui décrit le travail du worker.

```sh
docker run --rm -v /data/nyx/camels/camelworker1/camel-context.xml:/app/camel-context.xml \
  snuids/camel4:v1.2
```

## Version

**Une seule version, partout : `v1.2`.**

| où | comment |
|---|---|
| tag de l'image | `snuids/camel4:v1.2`, pris dans la **première ligne** du message de commit |
| label OCI | `org.opencontainers.image.version`, posé à la construction |
| variable d'environnement | `CAMEL_WORKER_VERSION`, même valeur, surchargeable par le compose |
| lifesign | la route `ModuleInfo` publie `CAMEL_WORKER_VERSION` dans `NYX_MODULE_INFO` |

La règle existe à cause d'un vieux défaut : la version du lifesign était écrite **en dur** dans le
fichier de routes (`"version": "v1.0.1"`), donc elle ne bougeait pas quand l'image changeait — le
module annonçait v1.0.1 aussi bien en v0.15 qu'en v0.19. Si tu changes la version, change-la à la
construction (le tag) et laisse le compose suivre :

```yaml
  camelworker1:
    image: snuids/camel4:v1.2
    environment:
      - workflow=CamelWorker1
      - CAMEL_WORKER_VERSION=v1.2   # même valeur que le tag
```

## Construire

```sh
docker build --build-arg VERSION=v1.2 --build-arg REVISION=$(git rev-parse --short HEAD) \
  -t snuids/camel4:v1.2 .
```

La CI (`.github/workflows/build-container.yml`) fait exactement cela : elle prend la première ligne
du message de commit, refuse de construire si ce n'est pas une version, construit avec les deux
arguments et pousse l'image.

Le jar est toujours `target/app.jar` (nom fixe, `<finalName>app</finalName>`) : le `Dockerfile` ne
dépend pas du numéro de version.

## Ce que fait le chargeur

`com.config.CamelContextLoader` démarre le contexte Spring/Camel, puis boucle toutes les 5 secondes :
il publie l'état des routes sur `jms:topic:ROUTE_STATUS` (avec les en-têtes `workflow` et `serverId`).
Si le `camel-context.xml` monté est modifié, il s'arrête volontairement (`System.exit(1)`) pour que
le conteneur redémarre avec les nouvelles routes.
