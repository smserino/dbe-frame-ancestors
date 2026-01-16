// @ts-nocheck
const { parse } = require('csv-parse/sync')
const fs = require('fs')
const path = require('path')

module.exports = (() => {
  const records = parse(fs.readFileSync(path.join(__dirname, 'property-websites.csv'), 'utf8'), {
    columns: true,
    skip_empty_lines: true
  })

  const spidWebsiteMap = new Map()

  records.forEach(columns => {
    const { siteminder_property_id: spid, home_page_link: website } = columns
    const propertyWebsite = website.trim() !== 'null' && website.trim() !== '' ? website.trim() : null
    spidWebsiteMap.set(spid, propertyWebsite)
  })

  return spidWebsiteMap
})();
