// https://siteminder-jira.atlassian.net/wiki/spaces/SD/pages/244482143/Updating+config+for+booking+engine+allowed+frame-ancestors#Additional-domains-to-allow-to-support-specific-platforms


const wixDomains: string[] = ['filesusr.com']
const googleSitesDomains: string[] = ['googleusercontent.com', 'gstatic.com', 'sites.google.com']

interface DecorateConfigOptions {
  websiteBuilder?: string
}

export const decorateConfigValue = (
  domainSet: Set<string>,
  options: DecorateConfigOptions = {}
): string => {
  const domains: string[] = Array.from(domainSet)

  let { websiteBuilder } = options
  if (!websiteBuilder) {
    if (domains.some(domain => wixDomains.includes(domain))) {
      websiteBuilder = 'Wix'
    } else if (domains.some(domain => googleSitesDomains.includes(domain))) {
      websiteBuilder = 'Google Sites'
    }
  }

  if (websiteBuilder?.toLowerCase().includes('wix')) {
    wixDomains.forEach(domain => domainSet.add(domain))
  } else if (websiteBuilder?.toLowerCase().includes('google')) {
    googleSitesDomains.forEach(domain => domainSet.add(domain))
  }

  const domainList =  Array.from(domainSet).map(d => d.trim())
  return domainList.concat('*').join(',')
}
