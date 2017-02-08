# Execution de code généré lors pendant l'execution


## Introduction

L'une des évolutions majeures apportées par Spark 2.0 est le [Whole-Stage Code Genration](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-sql-whole-stage-codegen.html).

Cette petite démonstration qui va nous emener dans les entrailles de la JVM va se dérouler en 3 temps : 

* Dans un premier temps, nous allons définir un petit langage (comparable à du SQL) qui va nous permettre d'exprimer un certain nombre d'opération que l'on veut réaliser sur un ensemble de `String`;
* 
* Dans un premier temps, nous allons apprendre à compiler et executer un code pendant le runtime;
* Ensuite, nous allons 

Une fois n'est pas contume, nous allons prendre le problème à l'envers : avant de nous péoccuper sur le code à générer, nous allons d'abord nous assurer qu'il est possible d'exécuter avec la JVM du code généré au runtime. 


## 

## Back to Basics
* Java
* Class
* ClassLoader

## 

## Et dans la vraie vie
* Janino


## Et maintenant ? 
Nous avons vu que cette compilation à la volée permet d'aller un peu plus vite car on évite le invokedynamic

