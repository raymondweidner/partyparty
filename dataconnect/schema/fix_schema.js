const fs = require('fs');
const path = require('path');

const schemaPath = path.join(__dirname, 'schema.gql');
let schema = fs.readFileSync(schemaPath, 'utf8');

const regex = /(\s+)(\w+):\s*([A-Z]\w+)(!)?\s*@col\(name:\s*"([^"]+)"\)(\s*@index)?/g;
const scalars = new Set(['String', 'Boolean', 'Int', 'Float', 'Timestamp', 'Date', 'UUID']);

schema = schema.replace(regex, (match, space, fieldName, typeName, isRequired, snakeName, indexDirective) => {
    // If it's a scalar, we don't apply @ref logic
    if (scalars.has(typeName)) {
        return match;
    }

    const requiredStr = isRequired ? '!' : '';
    const indexStr = indexDirective ? ' @index' : '';
    
    const scalarRequired = isRequired ? '!' : '';
    const scalarField = `${fieldName}Id: UUID${scalarRequired} @col(name: "${snakeName}")${indexStr}`;
    const relationField = `${fieldName}: ${typeName}${requiredStr} @ref(fields: "${fieldName}Id")`;
    
    return `${space}${scalarField}${space}${relationField}`;
});

const relationsInUnique = ['member', 'tribe', 'source', 'subject', 'chat', 'poll', 'voter', 'pollEntry', 'meetup', 'meetupEvent', 'proposal', 'host', 'creator'];

relationsInUnique.forEach(rel => {
    const relRegex = new RegExp(`(@unique\\(fields:\\s*\\[[^\\]]*)"${rel}"([^\\]]*\\]\\))`, 'g');
    schema = schema.replace(relRegex, (match, prefix, suffix) => {
        return `${prefix}"${rel}Id"${suffix}`;
    });
});

fs.writeFileSync(schemaPath, schema);
console.log('Schema fixed successfully.');
