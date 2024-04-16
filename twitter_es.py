from elasticsearch import Elasticsearch, helpers
import snscrape.modules.twitter as sntwitter
from langdetect import detect
import configparser
import argparse

def create_index_if_not_exists(es, index_name):
    """
    Vérifie si l'index Elasticsearch existe et le crée s'il n'existe pas.
    """
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

def tweet_to_es_func(es, index_name, location, languages, maxTweets):
    """
    Effectue le scraping avec SNSCrape et enregistre les tweets dans Elasticsearch.
    """
    actions = []  # Liste pour stocker les actions d'indexation bulk

    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(f'near:"{location}"').get_items()):
        try:
            lang = detect(tweet.content)  # Détecte la langue du tweet
        except:
            lang = 'error'

        if i >= maxTweets:
            break

        if lang in languages:
            tweet_data = {
                "_index": index_name,
                "_id": tweet.id,
                "_source": {
                    "user_id": tweet.user.id,
                    "username": tweet.user.username,
                    "profil_created_at": tweet.user.created,
                    "followersCount": tweet.user.followersCount,
                    "friendsCount": tweet.user.friendsCount,
                    "location": tweet.user.location,
                    "tagname": tweet.user.displayname,
                    "profile_img": tweet.user.profileImageUrl,
                    "created_at": tweet.date,
                    "tweet_content": tweet.content,
                    "likes_count": tweet.likeCount,
                    "tweet_url": tweet.url,
                }
            }
            actions.append(tweet_data)

    # Utilisation de la méthode bulk pour l'indexation efficace de plusieurs documents
    helpers.bulk(es, actions)

def main(args):
    config = configparser.ConfigParser()
    config.read('config.ini')  # Charger les informations de connexion depuis un fichier de configuration

    try:
        # Récupérer les informations de connexion Elasticsearch depuis le fichier de configuration
        es_username = config.get('elasticsearch', 'username')
        es_password = config.get('elasticsearch', 'password')
        es_host = config.get('elasticsearch', 'host')
        es_port = config.get('elasticsearch', 'port')
        es_scheme = config.get('elasticsearch', 'scheme')

        # Création d'une connexion Elasticsearch sécurisée
        es = Elasticsearch(
            [es_host],
            http_auth=(es_username, es_password),
            scheme=es_scheme,
            port=es_port
        )

        # Nom de l'index Elasticsearch
        index_name = 'tweets_index'

        # Valider l'emplacement non vide
        if not args.location:
            print("L'emplacement ne peut pas être vide.")
            return

        # Créer l'index s'il n'existe pas
        create_index_if_not_exists(es, index_name)

        # Exécuter la fonction de scraping et d'enregistrement dans Elasticsearch
        tweet_to_es_func(es, index_name, args.location, args.languages, args.max_tweets)

    except Exception as e:
        print(f"Une erreur s'est produite lors de la connexion à Elasticsearch : {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraping et enregistrement de tweets dans Elasticsearch")
    parser.add_argument("--location", type=str, default=None, help="Emplacement pour la recherche de tweets")
    parser.add_argument("--languages", nargs="+", type=str, default=['fr', 'en'], help="Langues à filtrer")
    parser.add_argument("--max-tweets", type=int, default=20, help="Nombre maximal de tweets à récupérer")
    args = parser.parse_args()

    main(args)
