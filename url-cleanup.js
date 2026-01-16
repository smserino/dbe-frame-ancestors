// @ts-nocheck
const fs = require('fs')
const path = require('path')
const { config } = require('process')
const { parse } = require('csv-parse/sync')

const { internalDomains, domainsToFilterOut } = require('./domains')
const isDomainToFilterOut = (domain) => Boolean(domainsToFilterOut.find(d => domain === d || domain.endsWith('.' + d)))
const isInternalDomain = (domain) => Boolean(internalDomains.find(d => domain === d || domain.endsWith('.' + d)))

const propertyWebsites = require('./property-websites')

const setupFiles = () => {
  const inputFile = process.argv[2]
  if (!inputFile) {
    console.error('Usage: node url-cleanup.js <input-log_results>')
    process.exit(1)
  }

  const resultsDir = path.join(path.dirname(inputFile), 'results')
  if (!fs.existsSync(resultsDir)) {
    fs.mkdirSync(resultsDir, { recursive: true })
  }

  const logFile = path.join(resultsDir, 'log.txt')
  const summaryFile = path.join(resultsDir, 'summary.txt')
  const apacConfigFile = path.join(resultsDir, 'kvs-data-apac.json')
  const emeaConfigFile = path.join(resultsDir, 'kvs-data-emea.json')

  const files = fs.readdirSync(resultsDir)
  files.forEach(file => {
    const filePath = path.join(resultsDir, file)
    if (fs.statSync(filePath).isFile()) {
      fs.writeFileSync(filePath, '')
    }
  })

  return {
    inputFile,
    logFile,
    summaryFile,
    apacConfigFile,
    emeaConfigFile,
  }
}

// Returns a Map of (case-insensitive) channelCode => { channelCode, region, spid, referrers: Set } as taken from input file
const groupInputByChannelCode = (inputFile) => {
  const records = parse(fs.readFileSync(inputFile, 'utf8'), {
    skip_empty_lines: true
  })

  const byChannelCode = new Map()

  records.forEach(record => {
    const [region, spid, rawChannelCode, referrer] = record

    const channelCode = rawChannelCode.toLowerCase()
    byChannelCode.set(channelCode, {
      channelCode: rawChannelCode,
      region,
      spid,
      referrers: (byChannelCode.get(channelCode)?.referrers || new Set()).add(referrer.split('?')[0]),
    })
  })

  return byChannelCode
}

const generateConfig = ({ inputData, logFile }) => {

  const log = (...args) => {
    const message = args.join(' ') + '\n'
    fs.appendFileSync(logFile, message)
  }
  const configByRegion = { apac: [], emea: [] }

  inputData.forEach((data, channelCode) => {
    const { region, spid, referrers } = data

    const domains = extractDomainsFromUrls(referrers).filter(domain => domain && domain.trim().length > 0)

    const internalDomains = domains.filter(domain => isInternalDomain(domain))

    const filteredOutDomains = domains.filter(domain => isDomainToFilterOut(domain))

    const domainsForExclusion = [...internalDomains, ...filteredOutDomains]
    const finalDomains = domains.filter(domain => !domainsForExclusion.includes(domain))

    const website = propertyWebsites.get(spid) ?? ''
    const websiteDomain = extractDomainsFromUrls([website])[0]
    if (websiteDomain && !finalDomains.includes(websiteDomain) && !isInternalDomain(websiteDomain)) {
      finalDomains.unshift(websiteDomain)
    }

    log(`Evaluated channel code: ${channelCode}`)
    log(`--Region: ${region}`)
    log(`--SPID: ${spid}`)
    log(`--Referrers: ${Array.from(referrers).join(', ')}`)
    log(`--Domains: ${Array.from(domains).join(', ')}`)
    log(`----Internal domains found: ${internalDomains.join(', ')}`)
    log(`----Domains to filter out: ${filteredOutDomains.join(', ')}`)
    log(`----Website set in DB: ${website}`)
    log(`----Website domain extracted: ${websiteDomain}`)
    log(`--Final domains to include: ${finalDomains.join(', ')}`)
    log('\n')

    configByRegion[region.toLowerCase()].push({
      key: channelCode,
      value: [...finalDomains, '*'].join(','),
    })
  })

  return configByRegion
}

const createSummary = ({ inputData, summaryFile }) => {
  const log = (...args) => {
    const message = args.join(' ') + '\n'
    fs.appendFileSync(summaryFile, message)
  }

  const warn = (...args) => {
    log(...args)
    console.warn(...args)
  }

  log('Summary Report')
  log('==============\n')

  const spidList = Array.from(inputData.values()).map(d => `"${d.spid}"`)
  log(`Channel code total = ${inputData.size}`)
  log(`SPID total = ${spidList.length}`)
  log(`SPIDs\n${spidList.join(',')}\n`)

  if (inputData.size !== spidList.length) {
    log('==============')
    warn('Warning: Channel code count does not match SPID count. There may be duplicate SPIDs.')
  }

  const inputValues = Array.from(inputData.values())
  const needsRefetch = inputValues.some(({ spid }) => !propertyWebsites.has(spid))
  if (needsRefetch) {
    log('==============')
    warn(
      'Warning: Some SPIDs are missing website data.',
      `Please copy SPIDs from ${summaryFile}`,
      'and update filter in query in https://siteminder-pciprod.cloud.databricks.com/editor/queries/4166014845012662?o=7316601961078759 and re-run the query.',
      'Copy results as CSV to embedding.csv and re-run cleanup script.'
    )
  }

  const channelCodeNeedsLowerCaseConsideration = inputValues.filter(({ channelCode }) => channelCode.toLocaleLowerCase() !== channelCode.toLowerCase())
  if (channelCodeNeedsLowerCaseConsideration.length > 0) {
    log('==============')
    warn('Warning: Some channel codes have different results for toLocaleLowerCase vs toLowerCase. Please review:')
    channelCodeNeedsLowerCaseConsideration.forEach(({ channelCode }) => {
      warn(`-- ${channelCode} | toLocaleLowerCase: ${channelCode.toLocaleLowerCase()} | toLowerCase: ${channelCode.toLowerCase()}`)
    })
  }
}

const extractDomainsFromUrls = (urls) => {
  const domains = new Set()
  urls.forEach(urlString => {
    try {
      const url = new URL(urlString)
      domains.add(url.hostname.replace(/^www\./, ''))
    } catch (e) {
      // Ignore invalid URLs
    }
  })
  return Array.from(domains)
}

const main = () => {

  const { inputFile, logFile, summaryFile, apacConfigFile, emeaConfigFile } = setupFiles()

  const rawInputByChannelCode = groupInputByChannelCode(inputFile)

  const config = generateConfig({
    inputData: rawInputByChannelCode,
    logFile,
  })

  createSummary({
    inputData: rawInputByChannelCode,
    summaryFile,
  })

  fs.writeFileSync(apacConfigFile, JSON.stringify(config.apac, null, 2))
  fs.writeFileSync(emeaConfigFile, JSON.stringify(config.emea, null, 2))

}

main()
