/**
 * Created by clayton on 23/06/16.
 */
(() => {
  'use strict'

  const receita = require('./jobs/receita.json')
  const despesa = require('./jobs/despesa.json')
  const rp = require('request-promise')
  const HOST = process.arg[ 1 ] ? process.arg[ 1 ] : null
  const job = process.arg[ 2 ] ? process.arg[ 2 ] : 'receita'
  const year = process.arg[ 3 ] ? process.arg[ 3 ] : 2009
  let bodyJson = job === 'receita' ? receita : despesa

  /**
   * tratando o body para aplicar a data
   *
   **/
  let dateIni = new Date(Date.UTC(parseInt(year), 0))
  let dateEnd = new Date()
  bodyJson.dataSchema.granularitySpec.intervals = [ `${dateIni.toISOString().substring(0, 10)}/${dateEnd.toISOString().substring(0, 10)}` ]

  let options = {
    method: 'POST',
    uri: HOST,
    body: bodyJson,
    json: true // Automatically stringifies the body to JSON
  }

  rp(options)
    .then(function (parsedBody) {
      // POST succeeded...
    })
    .catch((err) => {
      // POST failed...
    })
})()
