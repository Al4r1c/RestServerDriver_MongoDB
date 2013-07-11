<?php
namespace AlaroxRestServeurDrivers\Drivers;

use AlaroxRestServeur\Serveur\Lib\ObjetReponse;
use AlaroxRestServeur\Serveur\Traitement\Data\AbstractDatabase;
use AlaroxRestServeur\Serveur\Traitement\Data\DatabaseConfig;
use AlaroxRestServeur\Serveur\Traitement\DonneeRequete\ChampRequete;
use AlaroxRestServeur\Serveur\Traitement\DonneeRequete\Operateur;
use AlaroxRestServeur\Serveur\Traitement\DonneeRequete\ParametresManager;
use Mandango\Cache\FilesystemCache;
use Mandango\Connection;
use Mandango\Document\Document;
use Mandango\Mandango;
use Mandango\MetadataFactory;
use Mandango\Repository;
use Mandango\Type\Container;
use MongoId;

class DatabaseMongo extends AbstractDatabase
{
    /**
     * @param MetadataFactory $metadataFactory
     * @param string $cacheFolder
     */
    public function __construct($metadataFactory, $cacheFolder)
    {
        $cache = new FilesystemCache($cacheFolder);
        $this->setConnection(new Mandango($metadataFactory, $cache));
    }

    /**
     * @param DatabaseConfig $databaseInformations
     */
    public function ouvrirConnectionDepuisFichier($databaseInformations)
    {
        if ($databaseInformations->getUsername() != '' && $databaseInformations->getPassword() != '') {
            $connection =
                new Connection(
                    'mongodb://' . $databaseInformations->getUsername() . ':' . $databaseInformations->getPassword() .
                    '@' . $databaseInformations->getHost() . ':' .
                    $databaseInformations->getPort(), $databaseInformations->getDatabase());
        } else {
            $connection =
                new Connection('mongodb://' . $databaseInformations->getHost() . ':' .
                $databaseInformations->getPort(), $databaseInformations->getDatabase());
        }

        $this->getConnection()->setConnection('myConnection', $connection);
        $this->getConnection()->setDefaultConnectionName('myConnection');
    }

    /**
     * @param string $repository
     * @return Repository
     */
    public function getRepository($repository = null)
    {
        if (is_null($repository)) {
            $repository = 'Model\\' . ucfirst(strtolower($this->getNomTable()));
        }

        return $this->getConnection()->getRepository($repository);
    }

    /**
     * @param array $tableauValeurs
     * @return array
     */
    private function mongoIdToString($tableauValeurs)
    {
        return array_map_recursive(
            function ($valeur) {
                if ($valeur instanceof MongoId) {
                    return $valeur->__toString();
                }

                return $valeur;
            },
            $tableauValeurs
        );
    }

    /**
     * @param string $id
     * @return ObjetReponse
     */
    public function recupererId($id)
    {
        if (is_null($id)) {
            return new ObjetReponse(400);
        }

        $article = $this->getRepository()->findOneById($id);

        if (!empty($article)) {
            return new ObjetReponse(200, array($this->getNomTable() => $this->recupererResultats($article)));
        } else {
            return new ObjetReponse(404);
        }
    }

    /**
     * @param ParametresManager $filtres
     * @return ObjetReponse
     */
    public function recuperer($filtres)
    {
        list($sort, $limit, $skip) = $this->gererTris($filtres);

        try {
            if (($tabFiltres = $this->recupererChampsEligiblesGet($filtres)) === false) {
                return new ObjetReponse(400);
            }

            $resultat =
                $this->getRepository()->createQuery()->criteria($tabFiltres)
                ->sort($sort)
                ->limit($limit)
                ->skip($skip);

            if ($resultat->count() > 0) {
                $tabResult[$this->getNomTable()] = array();

                foreach ($resultat as $unResultatTrouve) {
                    $tabResult[$this->getNomTable()][] = $this->recupererResultats($unResultatTrouve);
                }

                return new ObjetReponse(200, $tabResult);
            } else {
                return new ObjetReponse(200);
            }
        } catch (\InvalidArgumentException $e) {
            return new ObjetReponse(404);
        }
    }


    /**
     * @param ParametresManager $champs
     * @return ObjetReponse
     */
    public function inserer($champs)
    {
        $obj = $this->getConnection()->create($this->getRepository()->getDocumentClass())->fromArray(
                   $this->recupererChampsEligibles($champs)
               )->save();

        return new ObjetReponse(201, array($this->getNomTable() => $this->recupererResultats($obj)));
    }

    /**
     * @param string $idObjet
     * @param ParametresManager $champs
     * @return ObjetReponse
     */
    public function insererIdempotent($idObjet, $champs)
    {
        return new ObjetReponse(403);
    }

    /**
     * @param string $id
     * @param ParametresManager $champs
     * @return ObjetReponse
     */
    public function mettreAJour($id, $champs)
    {
        if (is_null($id)) {
            return new ObjetReponse(400);
        }

        $toUpdateObject = $this->getRepository()->findOneById($id);

        if (!is_null($toUpdateObject)) {
            foreach ($this->recupererChampsEligibles($champs) as $nomChamp => $valeurChamp) {
                $toUpdateObject->set($nomChamp, $valeurChamp);
            }

            $toUpdateObject->save();

            return new ObjetReponse(200);
        } else {
            return new ObjetReponse(404);
        }
    }

    /**
     * @param string $id
     * @return ObjetReponse
     */
    public function supprimerId($id)
    {
        if (is_null($id)) {
            return new ObjetReponse(400);
        }

        return $this->supprimer(array('_id' => new \MongoId($id)));
    }

    /**
     * @param array $filtres
     * @return ObjetReponse
     */
    public function supprimer($filtres = array())
    {
        $objectsList = $this->getRepository()->createQuery($filtres);

        if ($objectsList->count() > 0) {
            foreach ($objectsList as $singleObject) {
                $singleObject->delete();
            }

            return new ObjetReponse(200);
        } else {
            return new ObjetReponse(404);
        }
    }

    /**
     * @param array $id
     * @param string $nomCollection
     * @param ParametresManager $nouveauxChamps
     * @return ObjetReponse
     */
    public function setCollection($id, $nomCollection, $nouveauxChamps)
    {
        if (!is_array($resultCheckExist = $this->checkValidObject($id, $nomCollection))) {
            return $resultCheckExist;
        } else {
            list($champConcerne, $objetConcerne) = $resultCheckExist;
            $tabObjects = array();

            if (!is_null($champsConcerne = $nouveauxChamps->getUnChampsRequete($nomCollection))) {
                $foreignRepository =
                    $this->getRepository(
                        $this->getRepository()->getMetadata()['referencesMany'][$champConcerne['dbName']]['class']
                    );

                if (is_array($valeur = $champsConcerne->getValeurs())) {
                    foreach ($valeur as $uneValeur) {
                        if (!is_null($nouvelObjet = $foreignRepository->findOneById($uneValeur))) {
                            $tabObjects[] = $nouvelObjet;
                        }
                    }
                } else {
                    if (!is_null($nouvelObjet = $foreignRepository->findOneById($valeur))) {
                        $tabObjects[] = $nouvelObjet;
                    }
                }
            }

            $objetConcerne->{'get' . ucfirst($champConcerne['dbName'])}()->replace($tabObjects);
            $objetConcerne->save();

            return new ObjetReponse(200);
        }
    }

    /**
     * @param array $id
     * @param string $nomCollection
     * @param string $idNouvelObjet
     * @return ObjetReponse
     */
    public function ajouterDansCollection($id, $nomCollection, $idNouvelObjet)
    {
        if (!is_array($resultCheckExist = $this->checkValidObject($id, $nomCollection))) {
            return $resultCheckExist;
        } else {
            list($champConcerne, $objetConcerne) = $resultCheckExist;

            $foreignRepository =
                $this->getRepository(
                    $this->getRepository()->getMetadata()['referencesMany'][$champConcerne['dbName']]['class']
                );

            if (!is_null($nouvelObjet = $foreignRepository->findOneById($idNouvelObjet))) {
                $tabKey = array();

                foreach ($objetConcerne->{'get' . ucfirst($champConcerne['dbName'])}() as $t) {
                    $tabKey[] = $t->getId();
                }

                if (!in_array($idNouvelObjet, $tabKey)) {
                    $objetConcerne
                    ->{'add' . ucfirst($champConcerne['dbName'])}(
                            $nouvelObjet
                        )
                    ->save();
                }

                return new ObjetReponse(200);
            } else {
                return new ObjetReponse(404);
            }
        }
    }

    /**
     * @param string $id
     * @param string $nomCollection
     * @param string $idToDelObject
     * @return ObjetReponse
     */
    public function supprimerDansCollection($id, $nomCollection, $idToDelObject)
    {
        if (!is_array($resultCheckExist = $this->checkValidObject($id, $nomCollection))) {
            return $resultCheckExist;
        } else {
            list($champConcerne, $objetConcerne) = $resultCheckExist;

            $foreignRepository =
                $this->getRepository(
                    $this->getRepository()->getMetadata()['referencesMany'][$champConcerne['dbName']]['class']
                );

            if (!is_null($toDelObject = $foreignRepository->findOneById($idToDelObject))) {
                $objetConcerne
                ->{'remove' . ucfirst($champConcerne['dbName'])}(
                        $toDelObject
                    )
                ->save();

                return new ObjetReponse(200);
            } else {
                return new ObjetReponse(404);
            }
        }
    }

    /**
     * @param string $id
     * @param string $nomCollection
     * @return ObjetReponse
     */
    public function supprimerCollection($id, $nomCollection)
    {
        if (!is_array($resultCheckExist = $this->checkValidObject($id, $nomCollection))) {
            return $resultCheckExist;
        } else {
            list($champConcerne, $objetConcerne) = $resultCheckExist;

            foreach ($list = $objetConcerne->{'get' . ucfirst($champConcerne['dbName'])}() as $toDelObject) {
                $list->remove($toDelObject);
            }

            $objetConcerne->save();

            return new ObjetReponse(200);
        }
    }

    /**
     * @param Mandango $connection
     * @return bool
     */
    public function fermerConnection($connection)
    {
        $connection->clearConnections();

        return count($connection->getConnections()) == 0;
    }

    /**
     * @param ChampRequete $uneDonneeRequete
     * @return mixed
     */
    private function appliquerOperateurs($uneDonneeRequete)
    {
        if (strcmp(($clef = $uneDonneeRequete->getChamp()), '_id') == 0) {
            return new \MongoId($uneDonneeRequete->getValeurs());
        } else {
            $metadataFields = $this->getRepository()->getMetadata()['fields'];

            if (array_key_exists($clef, $metadataFields)) {
                if (isset($metadataFields[$clef]['referenceField'])) {
                    return new \MongoId($uneDonneeRequete->getValeurs());
                } else {
                    $type = $this->getRepository()->getMetadata()['fields'][$uneDonneeRequete->getChamp()]['type'];

                    if (is_array($valeur = $uneDonneeRequete->getValeurs())) {
                        foreach ($valeur as $clef => $uneValeur) {
                            $valeur[$clef] = Container::get($type)->toPHP($uneValeur);
                        }

                        return $this->conditionAvecOr($uneDonneeRequete->getOperateur()->getType(), $valeur);
                    } else {
                        return $this->conditionSansOr(
                            $uneDonneeRequete->getOperateur()->getType(),
                            Container::get($type)->toPHP($uneDonneeRequete->getValeurs())
                        );
                    }
                }
            } else {
                return null;
            }
        }
    }

    /**
     * @param string $laCondition
     * @param array $valeur
     * @return mixed
     */
    private function conditionAvecOr($laCondition, $valeur)
    {
        switch ($laCondition) {
            case 'lt':
            case 'lte':
                $valeurMin = $valeur[0];
                unset($valeur[0]);

                foreach ($valeur as $unElem) {
                    if ($unElem < $valeurMin) {
                        $valeurMin = $unElem;
                    }
                }

                return array('$' . $laCondition => $valeurMin);
                break;
            case 'eqs':
                return array('$in' => $valeur);
                break;
            case 'gte':
            case 'gt':
                $valeurMax = $valeur[0];
                unset($valeur[0]);

                foreach ($valeur as $unElem) {
                    if ($unElem > $valeurMax) {
                        $valeurMax = $unElem;
                    }
                }

                return array('$' . $laCondition => $valeurMax);
                break;
            case 'like':
                return new \MongoRegex('/' . implode('|', $valeur) . '/i');
                break;
            case 'eq':
            default:
                $tabValeurs = array();
                foreach ($valeur as $uneValeur) {
                    $tabValeurs[] = $this->conditionSansOr($laCondition, $uneValeur);
                }

                return array('$in' => $tabValeurs);
        }
    }

    /**
     * @param string $laCondition
     * @param string $valeur
     * @return mixed
     */
    private function conditionSansOr($laCondition, $valeur)
    {
        switch ($laCondition) {
            case 'lt':
            case 'lte':
            case 'gte':
            case 'gt':
                return array('$' . $laCondition => $valeur);
                break;
            case 'eqs':
                return $valeur;
                break;
            case 'like':
                return new \MongoRegex('/' . $valeur . '/i');
                break;
            case 'eq':
            default:
                if (is_string($valeur)) {
                    return new \MongoRegex('/^' . $valeur . '$/i');
                } else {
                    return $valeur;
                }
        }
    }

    /**
     * @param ParametresManager $paramManager
     * @return array
     */
    private function gererTris($paramManager)
    {
        $sort = array();
        $limit = PHP_INT_MAX;
        $skip = 0;

        if (!is_null($orderBy = $paramManager->getUnTri('orderBy'))) {
            if (!is_null($orderWay = $paramManager->getUnTri('orderWay'))) {
                if (strcmp(strtolower($orderWay->getValeur()), 'desc') == 0) {
                    $order = -1;
                } else {
                    $order = 1;
                }
            } else {
                $order = 1;
            }

            $sort[$orderBy->getValeur()] = $order;
        }

        if (!is_null($limitResult = $paramManager->getUnTri('pageSize'))) {
            $limit = (int)$limitResult->getValeur();
        }

        if (!is_null($pageNum = $paramManager->getUnTri('pageNum'))) {
            $valeurPage = (int)$pageNum->getValeur();
            $skip = ($valeurPage >= 1 ? $valeurPage - 1 : 0) * $limit;
        }

        return array($sort, $limit, $skip);
    }

    /**
     * @param Document $article
     * @return array
     */
    private function recupererResultats($article)
    {
        $tabClefsEmbarquees = array();

        $metadata = $this->getRepository()->getMetadata();
        if (count($tabEmbedded = array_merge($metadata['embeddedsOne'], $metadata['embeddedsMany'])) > 0) {
            $tabClefsEmbarquees = array_keys($tabEmbedded);
        }

        $tabResult = $article->toArray(true);
        foreach ($tabClefsEmbarquees as $unChampEmbarque) {
            $tabResult[$unChampEmbarque] = array();

            foreach ($article->{'get' . ucfirst($unChampEmbarque)}() as $clefEmbarquee =>
                $itemEmbarquee) {
                $tabResult[$unChampEmbarque][$clefEmbarquee] = $itemEmbarquee->toArray();
            }
        }

        return $this->mongoIdToString($tabResult);
    }

    /**
     * @param ParametresManager $champs
     * @return array
     */
    private function recupererChampsEligiblesGet($champs)
    {
        $tabChamps = array();

        $metadata = $this->getRepository()->getMetadata();

        foreach ($champs->getChampsRequete() as $uneDonneeRequete) {
            if (!is_null($donneAvecConditions = $this->appliquerOperateurs($uneDonneeRequete))) {

                if (isset($metadata['fields'][$nomChamp = $uneDonneeRequete->getChamp()]['referenceField'])) {
                    $nomChamp = $metadata['fields'][$uneDonneeRequete->getChamp()]['dbName'];
                }

                if (!isset($tabChamps[$nomChamp])) {
                    $tabChamps[$nomChamp] = $donneAvecConditions;
                } else {
                    $tabChamps[$nomChamp] =
                        $donneAvecConditions + $tabChamps[$nomChamp];
                }
            } else {
                return false;
            }
        }

        return $tabChamps;
    }

    /**
     * @param ParametresManager $champs
     * @return array
     */
    private function recupererChampsEligibles($champs)
    {
        $tabChamps = array();

        $metadata = $this->getRepository()->getMetadata();

        foreach ($champs->getChampsRequete() as $uneDonneeRequete) {
            if (array_key_exists(($nomChamp = $uneDonneeRequete->getChamp()), $metadata['fields'])) {
                $champConcerne = $metadata['fields'][$nomChamp];

                if (isset($champConcerne['referenceField'])) {
                    if (isset($metadata['referencesOne'][$champConcerne['dbName']])) {
                        $foreignRepository =
                            $this->getRepository($metadata['referencesOne'][$champConcerne['dbName']]['class']);

                        if (!is_null(
                            $object = $foreignRepository->findOneById($valeur = $uneDonneeRequete->getValeurs())
                        )
                        ) {
                            $tabChamps[$nomChamp] = $object->getId();
                        } elseif ($valeur == '0' || $valeur == 'false' || $valeur == 'null') {
                            $tabChamps[$nomChamp] = false;
                        }
                    } else {
                        $foreignRepository =
                            $this->getRepository($metadata['referencesMany'][$champConcerne['dbName']]['class']);

                        $nouvelleValeur = array();

                        if (is_array($uneDonneeRequete->getValeurs())) {
                            foreach ($uneDonneeRequete->getValeurs() as $uneValeur) {
                                if (!is_null($object = $foreignRepository->findOneById($uneValeur))) {
                                    $nouvelleValeur[] = $object->getId();
                                }
                            }
                        } else {
                            if (!is_null($object = $foreignRepository->findOneById($uneDonneeRequete->getValeurs()))) {
                                $nouvelleValeur[] = $object->getId();
                            }
                        }

                        if (!empty($nouvelleValeur)) {
                            $tabChamps[$nomChamp] = $nouvelleValeur;
                        } else {
                            $tabChamps[$nomChamp] = false;
                        }
                    }
                } else {
                    $tabChamps[$nomChamp] = $uneDonneeRequete->getValeurs();
                }
            }
        }

        return $tabChamps;
    }

    /**
     * @param string $id
     * @param string $nomCollection
     * @return array|ObjetReponse
     */
    private function checkValidObject($id, $nomCollection)
    {
        $metadata = $this->getRepository()->getMetadata();

        if (array_key_exists($nomCollection, $metadata['fields'])) {
            $champConcerne = $metadata['fields'][$nomCollection];

            if (isset($champConcerne['referenceField']) && isset($metadata['referencesMany'][$champConcerne['dbName']])
            ) {
                if (!is_null($objetConcerne = $this->getRepository()->findOneById($id))) {
                    return array($champConcerne, $objetConcerne);
                } else {
                    return new ObjetReponse(404);
                }
            } else {
                return new ObjetReponse(403);
            }
        } else {
            return new ObjetReponse(404);
        }
    }
}