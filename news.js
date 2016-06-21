const elasticsearch = require('elasticsearch')
const Promise = require('bluebird')
const dbNews = require('./db.json')
const request = require('request-promise')

// ElasticSearch
const newsIndex = 'news'
const newsType = 'news'
const lastUpdateIndex = 'news-last-update'
const lastUpdateType = 'news-last-update'

// Orchard
const orchardApi = process.env.ORCHARD_API || 'http://orchard.dchm.es.gov.br/api/'
const sitesEndpoint = `${orchardApi}noticias/getsitelist`
const newsEndpoint = `${orchardApi}noticias/GetNoticiasBySite`
const maxNews = 50; // Should reflect maximum number of news orchard's API responds

const client = new elasticsearch.Client({
    host: 'http://es.labs.prodest.dcpr.es.gov.br',
    log: 'error'
})

var exNews = {
    'noticiaId': '30278',
    'titulo': 'Peça “O Menino que Mordeu Picasso” para alunos da rede estadual',
    'resumo': null,
    'body': 'Vivian Camargo / Flávia Zambrone / Fernanda',
    'urlNoticia': 'sedu.dchm.es.gov.br/peca-o-menino-que-mordeu-picasso-para-alunos',
    'urlImagemDestaque': 'http://orchard.dchm.es.gov.br/Media/sedu/Importacao/Noticias/P3160341.gif',
    'tags': [],
    'dataCriacao': '2016-03-17T10:30:00Z',
    'dataModificacao': '2016-03-17T10:37:01Z',
    'dataPublicacao': '2016-04-11T18:54:16.4608839Z',
    'imagensCarrossel': [],
    'publicado': true,
    'siglaSite': 'sedu'
}

function getOrchardSites() {
    const options = {
        uri: sitesEndpoint,
        headers: {
            'User-Agent': 'Request-Promise'
        },
        json: true
    }

    return request(options)
}

function getLastUpdate(site) {

    return client.indices.exists({
        index: lastUpdateIndex
    })
        .then(existsIndex => {
            if (!existsIndex) {
                return client.indices.create({
                    index: lastUpdateIndex
                })
            } else {
                return Promise.resolve();
            }
        })
        .then(a => {
            return client.get({
                index: lastUpdateIndex,
                type: lastUpdateType,
                id: site
            })
        })
        .then(lastUpdate => {
            return Promise.resolve(new Date(lastUpdate._source.date));
        })
        .catch(err => {
            return Promise.resolve(new Date(1900, 01, 01));
        });
}

function getOrchardNews(site, lastUpdate) {
    const options = {
        uri: newsEndpoint,
        headers: {
            'User-Agent': 'Request-Promise'
        },
        qs: {
            sitename: site,
            dataUltimaVerificacao: lastUpdate.toISOString().slice(0, -1)
        },
        json: true
    };

    return request(options);
}

function bulkIndexNews(site, lastUpdate) {
    return new Promise((resolve, reject) => {
        getOrchardNews(site, lastUpdate)
            .then(news => {
                var actions = news.map(n => {
                    return {
                        index: {
                            _index: newsIndex,
                            _type: newsType,
                            _id: `${n.siglaSite}_${n.noticiaId}`
                        }
                    }
                });

                const body = [];
                for (let i = 0; i < actions.length; i++) {
                    body.push(actions[i]);
                    body.push(news[i]);
                }

                if (news.length > 0) {
                    lastUpdate = new Date(news[news.length - 1].dataModificacao);

                    return client.bulk({
                        body: body
                    });
                }
                else {
                    return Promise.resolve();
                }
            })
            .then(bulk => {
                if (bulk) {
                    client.index({
                        index: lastUpdateIndex,
                        type: lastUpdateType,
                        id: site,
                        body: {
                            date: lastUpdate
                        }
                    });

                    if (bulk.items.length === maxNews) {
                        return bulkIndexNews(site, lastUpdate);
                    }
                }

                console.log(`Updated all news from ${site}.`);
                return resolve(`Updated all news from ${site}.`);
            })
            .catch(err => {
                return reject(err);
            });
    });
}

function getAndIndexNews(site) {
    return getLastUpdate(site)
        .then(lastUpdate => {
            return bulkIndexNews(site, lastUpdate);
        });
}

client.indices.exists({
    index: newsIndex
})
    .then(existsIndex => {
        if (!existsIndex) {
            return client.indices.create({
                index: newsIndex
            });
        } else {
            return Promise.resolve();
        }
    })
    .then(a => {
        return getOrchardSites();
    })
    .then(sites => {
        const promises = []
        sites.forEach(site => {
            promises.push(getAndIndexNews(site.sigla));
        });

        return Promise.all(promises);

        // Promise.reduce(promises, (a, b) => {
        //     return b.then(a => console.log(a));
        // });
    })
    .then(a => {
        console.log('Fim.');
        process.exit(0);
    })
    .catch(err => {
        console.log('Erro:\n' + err);
        process.exit(-1);
    });
    