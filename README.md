# Global workflow

#### Required before testing


### Analytical steps

1) Analyze a short sample of data
--> Test & compare python methods through jupyter notebook

a) Get all of fields
b) Do this
c) Do that

2) Test

#################################################### WorkFlow PYSPARK ####################################################

# 1ère étape : PARSING DES FICHIERS EXTRAITS

Passer respectivement les scripts "parsing_Fulltext.py" et "parsing_TitreCpc.py" sur les fichiers "fulltexts" et "titre_cpc"
Pour le fichier "Fulltext", aucun problème de mémoire puisque les opérations sont realisées ligne par ligne. Le script peut donc s'exécuter sur chaque partie de l'export ou après concaténation. Je l'ai fait avant la concaténation. Dans le cas inverse, se référer directement à la troisème partie.
Pour le fichier "Titre_CPC", les opérations s'éxécutent en mémoire puisque j'avais splité ce fichier d'environ 6 Go au préalable.

# 2ième étape : PRE-PROCESSING DES FICHIERS PARSÉS

Aucun problème de mémoire pour ces deux opérations qui s'exécutent ligne par ligne.
Pour le fichier "Fulltext" --> "preprocessing_fulltext.py". On retire tout caractère non-littéraire comme les balises html, la ponctuation ou encore les chiffres présents.
Pour le fichier "Titre_CPC" --> "preprocessing_titrecpc.py". On récupère les premiers caractères de la classe CPC permettant d'identifier les grandes catégories. Le choix reste important car on remarquera plus tard, après jointure qu'environ 800 catégories sont représentées.

# 3ième étape : CONCATÉNATION DES FICHIERS FULLTEXT SUR SPARK

Pour cette étape, se référer au fichier "Code Pyspark.csv"
Le résultat est enregistré sur HDFS

# 4ième étape : JOINTURE

Idem

# 5ième étape : NETTOYAGE DU RESULTAT DE JOINTURE

À la suite de l'enregistrement en base, quelques caractères non littéraux apparaissent.
La solution choisie fut d'imoorter le fichier, de lui passer le script "preprocessing_dataset.py", et ensuite de le recharger sur HDFS.
Idealement il faut revoir l'écriture du résultat pour éviter l'apparition de ces caractères.

# 6ième étape : MACHINE LEARNING

Pour cette étape, se référer au fichier "Code Pyspark.csv".
A noter que le code permettant de retirer les stopwords avant la TF-IDF ne fonctionne pas pour du texte français.
Ceci explique,en partie, les mauvais résultats de l'algorithme RANDOM FOREST.

# TESTS EN LOCAL

"dataset_homogene.py" --> À la fin de l'étape 5, au lieu de recharger le fichier sur HDFS, passer ce script pour extraire un dataset composé uniquement des 5 classes les plus représentées, avec respectivement 10000 représentants dans chaque classe.
"dataset_train_predict.py" -->  Puis, passer ce script sur le fichier obtenu pour créer deux fichiers de "train" et de "predict" pour l'implémentation des algorithmes de classification.
Le fichier de "train" sera composé de 8000 éléments de chaque classe, tandis que le fichier de "predict" sera lui composé de 2000 éléments de chaque classe.