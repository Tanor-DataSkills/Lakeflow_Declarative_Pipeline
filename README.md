# Retail project using Lakeflow_Declarative_Pipeline


Ce projet est réalisé pour mettre en évidence la caapcité à développer un projet de data engineering de bout en bout avec Databricks en utilisant une approche appelée Lakeflow_Declarative_Pipeline. Pour bien montrer les fonctionnalité de Databricks on travaillera sur des données de retail.


### On utilisera des free ressources

## Usecase

### Background

- Les entreprises de retail génère une grosse volumétrie de données de CRM, d'inventaire et de transaction.
- Nous disposons des données issues de sources et de formats variés; ce qui rend compliqué l'analyse unifiée.
- Les équipes métier ont besoin de kpi(quick insight) dans les ventes(sales), les customers, products et inventory.

### Problem

- Les données sont réparties dans plusieurs sources déconnectées entre elles; ceci crée data silos.
- Les anciennes pipelines existantes sont devenues lentes avec le temps et leur mise en echelle est difficile.
- Les métiers ont besoin d'instaurer une politique de self service BI analytic pour les opération de retail et des indicateurs de vente à travers des dashboards et Genie AI interface.

### Solution approach

- Créer une plateforme d'analyse de retail(**retail analytic plateform**) dans Databricks en utilisant une architecture de medaille.
- Intégrer des données depuis salesforce, postgreSQL et Azure blob storage
- Créer un retail Dashboard et Databricks genie AI interface permettant aux end users de faire leurs propres requêtes sur la plateforme.
  
## Architecture

<img width="925" height="454" alt="image" src="https://github.com/user-attachments/assets/5cbc02d9-6512-448d-8a38-61d00d5b8caa" />



## Generate code to read a CSV file into a DataFrame 


### ###
