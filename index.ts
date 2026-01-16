import { parse } from 'csv-parse/sync'
import * as fs from 'fs'
import * as path from 'path'
import { decorateConfigValue } from './config-value-decorator'
import { domainsToFilterOut, internalDomains } from './domains'
import propertyWebsites from './property-websites'
import typeFormResponses from './typeform-responses'

interface LogData {
  channelCode: string
  region: string
  spid: string
  referrers: Set<string>
}

interface ConfigEntry {
  key: string
  value: Set<string>
}

interface ConfigByRegion {
  apac: ConfigEntry[]
  emea: ConfigEntry[]
}

interface ProcessFiles {
  inputFile: string
  logFile: string
  summaryFile: string
  apacConfigFile: string
  emeaConfigFile: string
}

const isDomainToFilterOut = (domain: string): boolean => {
  return domainsToFilterOut.some(d => domain === d || domain.endsWith('.' + d))
}

const isInternalDomain = (domain: string): boolean => {
  return internalDomains.some(d => domain === d || domain.endsWith('.' + d))
}

const setupFiles = (): ProcessFiles => {
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

const groupInputByChannelCode = (inputFile: string): Map<string, LogData> => {
  const records = parse(fs.readFileSync(inputFile, 'utf8'), {
    skip_empty_lines: true
  }) as string[][]

  const byChannelCode = new Map<string, LogData>()

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

const generateConfigFromLogs = ({ inputData, logFile }: { inputData: Map<string, LogData>, logFile: string }): ConfigByRegion => {

  const log = (...args: any[]): void => {
    const message = args.join(' ') + '\n'
    fs.appendFileSync(logFile, message)
  }
  const configByRegion: ConfigByRegion = { apac: [], emea: [] }

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

    const regionKey = region.toLowerCase() as 'apac' | 'emea'
    configByRegion[regionKey].push({
      key: channelCode,
      value: new Set(finalDomains)
    })
  })

  return configByRegion
}

const updateConfigWithTypeformResponses = (config: ConfigByRegion): ConfigByRegion => {
  typeFormResponses.forEach(response => {
    const regionKey = response.region.toLowerCase() as 'apac' | 'emea'
    const channelCode = response.key.toLowerCase()
    const existingEntryIndex = config[regionKey].findIndex(entry => entry.key === channelCode)

    if (response.__remove) {
      // If not embedding, remove any existing entry
      if (existingEntryIndex !== -1) {
        config[regionKey].splice(existingEntryIndex, 1)
      }
    } else {
      const newValue = new Set(response.value.split(',').map(item => item.trim()))
      if (existingEntryIndex !== -1) {
        // Update existing entry with value from Typeform response
        config[regionKey][existingEntryIndex].value = newValue
      } else {
        // Add new entry
        config[regionKey].push({
          key: channelCode,
          value: newValue,
        })
      }
    }
  })
  return config
}

const validate = ({ inputData, summaryFile }: { inputData: Map<string, LogData>, summaryFile: string }): void => {
  const log = (...args: any[]): void => {
    const message = args.join(' ') + '\n'
    fs.appendFileSync(summaryFile, message)
  }

  const warn = (...args: any[]): void => {
    log(...args)
    console.warn(...args)
  }

  const inputValues = Array.from(inputData.values())

  const spidList = inputValues.map(d => `"${d.spid}"`)
  log(`List of SPIDs\n${spidList.join(',')}\n`)

  const channelCodesBySpid: Map<string, string[]> = new Map()
  inputValues.forEach(({ spid, channelCode }) => {
    const existing = channelCodesBySpid.get(spid) || []
    existing.push(channelCode)
    channelCodesBySpid.set(spid, existing)
  })

  // Check for SPIDs associated with multiple channel codes
  const spidsWithMultipleChannelCodes = Array.from(channelCodesBySpid.entries()).filter(([, channelCodes]) => channelCodes.length > 1)
  if (spidsWithMultipleChannelCodes.length > 0) {
    warn('Warning: Some SPIDs are associated with multiple channel codes. Please review.')
    spidsWithMultipleChannelCodes.forEach(([spid, channelCodes]) => {
      warn(`-- SPID: ${spid} | Channel codes: ${channelCodes.join(', ')}`)
    })
  }

  // Check for SPIDs missing from propertyWebsites
  const needsRefetch = inputValues.some(({ spid }) => !propertyWebsites.has(spid))
  if (needsRefetch) {
    warn(
      'Warning: Some SPIDs are missing website data.',
      `Please copy SPIDs from ${summaryFile}`,
      'and update filter in query in https://siteminder-pciprod.cloud.databricks.com/editor/queries/4166014845012662?o=7316601961078759 and re-run the query.',
      'Copy results as CSV to embedding.csv and re-run cleanup script.'
    )
  }

  // Check for channel codes with locale-specific case differences since we can only use toLowerCase in the CloudFront function
  const channelCodeNeedsLowerCaseConsideration = inputValues.filter(({ channelCode }) => channelCode.toLocaleLowerCase() !== channelCode.toLowerCase())
  if (channelCodeNeedsLowerCaseConsideration.length > 0) {
    warn('Warning: Some channel codes have different results for toLocaleLowerCase vs toLowerCase. Please review.')
    channelCodeNeedsLowerCaseConsideration.forEach(({ channelCode }) => {
      warn(`-- ${channelCode} | toLocaleLowerCase: ${channelCode.toLocaleLowerCase()} | toLowerCase: ${channelCode.toLowerCase()}`)
    })
  }

  // Validate typeform responses for duplicate channel codes
  const typeformChannelCodes = typeFormResponses.map(r => r.key.toLowerCase())
  const duplicateChannelCodes = typeformChannelCodes.filter((code, index) => typeformChannelCodes.indexOf(code) !== index)
  if (duplicateChannelCodes.length > 0) {
    warn('Warning: Duplicate channel codes found in typeform responses. Please review and update the typeform-responses.ts data.')
    duplicateChannelCodes.forEach(channelCode => {
      typeFormResponses.filter(r => r.key.toLowerCase() === channelCode).forEach((typeFormResponse) => {
        warn(`-- ${channelCode} | entry: ${JSON.stringify(typeFormResponse)}`)
      })
    })
  }
}

const extractDomainsFromUrls = (urls: string[] | Set<string>): string[] => {
  const domains = new Set<string>()
  urls.forEach((urlString: string) => {
    try {
      const url = new URL(urlString)
      domains.add(url.hostname.replace(/^www\./, ''))
    } catch (e) {
      // Ignore invalid URLs
    }
  })
  return Array.from(domains)
}

const main = (): void => {

  const { inputFile, logFile, summaryFile, apacConfigFile, emeaConfigFile } = setupFiles()

  const rawInputByChannelCode = groupInputByChannelCode(inputFile)

  validate({
    inputData: rawInputByChannelCode,
    summaryFile,
  })

  const config = generateConfigFromLogs({
    inputData: rawInputByChannelCode,
    logFile,
  })

  updateConfigWithTypeformResponses(config)

  const apacConfig = config.apac.map(entry => ({
    key: entry.key,
    value: decorateConfigValue(entry.value),
  }))
  const emeaConfig = config.emea.map(entry => ({
    key: entry.key,
    value: decorateConfigValue(entry.value),
  }))

  fs.writeFileSync(apacConfigFile, JSON.stringify(apacConfig, null, 2))
  fs.writeFileSync(emeaConfigFile, JSON.stringify(emeaConfig, null, 2))

}

main()
