// this should serve as a simple starting point for inserting InvertirOnline data into the database

const XLSX = require('xlsx');
const fsp = require('./fsp.js');
const fs = require('fs');
const csv = require('csv-parser');
const moment = require('moment');
const replace = require('replace-in-file');

const convertArgFilesToCsv = () => {
    return new Promise ((resolve, reject) => {
        fsp.readDir('data/ARG')
        .then(dir => {
            dir.forEach(fileName => {
                if (fileName.includes('.xls')) {
                    let accountId = fileName.split('.', 1);
                    if (accountId != '') {
                        let file = XLSX.readFile(`./data/arg/${fileName}`, { raw: true });
                        var stream = XLSX.stream.to_csv(file.Sheets.Sheet1, { FS: '|' });
                        stream.pipe(fs.createWriteStream(`./data/ARG/${accountId}.csv`));
                        stream.on('error', (e) => console.log(e) || reject(e));
                        stream.on('end', () => resolve());
                    }
                }
            });
        })
        .catch(e => console.log(e) || reject(e))
    })
}

//0.005 por share + 39.95 (cada 2000)
//0.005 por share + 22.5 (cada 2000)
async function parseCsvAndGenerateFinalFileArg () {
    return new Promise ((resolve, reject) => {
        try {
            fsp.readDir('data/arg')
            .then(dir => {
                dir.forEach(file => {
                    if (file.includes('.csv')) {
                        let fileId = file.split('.', 1);
                        console.log('Reading ARG', `${file}`)
                        fs.createReadStream(`./data/ARG/${file}`)
                        .pipe(csv({
                            separator: '|',
                            headers:[
                                'nromov', 
                                'nrobol', 
                                'tipomov', 
                                'concert', 
                                'liquid', 
                                'estado', 
                                'canttitulos', 
                                'precio', 
                                'comision', 
                                'iva', 
                                'otros', 
                                'monto', 
                                'observ', 
                                'tipocta']
                            }))
                        .on('data', data => {
                            fsp.append(`./data/ops.csv`, 
                                [fileId,
                                data.nromov,
                                moment(data.liquid, 'DD/MM/YYYY').isValid() ? moment(data.liquid, 'DD/MM/YYYY').format('YYYY-MM-DD') : moment(data.concert, 'DD/MM/YYYY').format('YYYY-MM-DD'),
                                data.monto.replace(/\./g,'').replace(/,/g, '.'),
                                parseFloat(data.comision.replace(/\./g,'').replace(/,/g, '.')) + parseFloat(data.iva.replace(/\./g,'').replace(/,/g, '.')),
                                data.tipocta,
                                (data.tipocta.toString().includes('Dolares')) ? 'usd': 'ars',
                                parseFloat(data.otros.replace(/\./, '').replace(/,/g, '.')),
                                parseFloat(data.precio.replace(/\./, '').replace(/,/g, '.')),
                                parseFloat(data.canttitulos.replace(/\./, '').replace(/,/g, '.')),
                                data.tipomov,
                                'internal\n']
                                .join('|')
                            );
                        })
                        .on('error', console.log || reject)
                        .on('end', resolve);
                    }
                });
            })
        } catch (e) {
            console.log(e);
        };
    })
}

async function parseCsvAndGenerateFinalFileUsa () {
    return new Promise ((resolve, reject) => {
        try {
            fsp.readDir('data/usa')
            .then(dir => {
                dir.forEach(file => {
                    if (file.includes('.csv')) {
                        console.log('Reading USA', `${file}`)
                        fs.createReadStream(`./data/USA/${file}`)
                        .pipe(csv())
                        .on('data', data => {
                            if (data.transaction_date){
                                fsp.append(`./data/ops.csv`, 
                                    [data.customer,
                                    '0',
                                    moment(new Date(data.transaction_date || data.entry_date)).format('YYYY-MM-DD'),
                                    data.amount,
                                    data.commission,
                                    data.description,
                                    'usd',
                                    data.fees,
                                    data.price,
                                    data.quantity,
                                    data.symbol,
                                    'internal\n']
                                    .join('|')
                                );
                            }
                        })
                        .on('error', console.log || reject)
                        .on('end', resolve);
                    }
                });
            })
        } catch (e) {
            console.log(e);
        };
    })
}

const deleteAllFinalFiles = () => {
    return Promise.all([
        fsp.unlink('./data/operations.sql'), 
        fsp.unlink('./data/ops.csv'), 
        fsp.unlink('./data/accounts.sql')])
    .then(() => console.log('Deleted final files'))
    .catch(console.log)
}

async function opsSqlCodeGenerator () {
    return new Promise ((resolve, reject) => {
        try {
            fsp.append(`./data/operations.sql`,
                `TRUNCATE some_schema.operations;
                START TRANSACTION;\n
                INSERT INTO some_schema.operations
                (accountid, op_id, operation_date, amount, commission, description, currency, fee, price, quantity, symbol, op_type)\n
                VALUES\n`)
            .then(() => {
                fs.createReadStream(`./data/ops.csv`)
                .pipe(csv({
                    separator: '|',
                    headers:[
                        'accountid', 
                        'op_id', 
                        'operation_date', 
                        'amount', 
                        'commission', 
                        'description', 
                        'currency', 
                        'fee', 
                        'price', 
                        'quantity', 
                        'symbol', 
                        'op_type']
                    }))
                .on('data', data => {
                    let values = '';
                    Object.keys(data).forEach(value => values += `'${data[value]}',`);
                    values = values.slice(0, -1);
                    fsp.append(`./data/operations.sql`,
                        `(${values}),\n`)
                })
                .on('error', console.log || reject)
                .on('end', () => {
                    resolve(fsp.append(`./data/operations.sql`,
                        `COMMIT;`))
                    });
            })
        }
        catch (e) {
            console.log(e);
        }
    })
}

async function accountsSqlCodeGenerator () {
    return new Promise ((resolve, reject) => {
        try {
            fsp.append(`./data/accounts.sql`,
                `TRUNCATE some_schema.accounts;
                START TRANSACTION;\n
                INSERT INTO some_schema.accounts
                (market, accountId, name)\n
                VALUES\n`)
            .then(() => {
                fs.createReadStream(`./data/accounts.csv`)
                .pipe(csv({
                    separator: ',',
                    // headers:[
                    //     'market', 
                    //     'accountId', 
                    //     'name']
                    }))
                .on('data', data => {
                    let values = '';
                    Object.keys(data).forEach(value => values += `'${data[value]}',`);
                    values = values.slice(0, -1);
                    fsp.append(`./data/accounts.sql`,
                        `(${values}),\n`)
                })
                .on('error', console.log || reject)
                .on('end', () => {
                    resolve(fsp.append(`./data/accounts.sql`,
                        `COMMIT;`))
                    });
            })
        }
        catch (e) {
            console.log(e);
        }
    })
}

async function finishQueryBuilder () {
    let options = {
        files: `./data/operations.sql`,
        from: `,\nCOMMIT;`,
        to: `;\nCOMMIT;`,
    };
    replace(options)
    .then(changes => {
        console.log('Modified files:', changes.join(', '));
    })
    .then(() => {
        options = {
            files: `./data/accounts.sql`,
            from: `,\nCOMMIT;`,
            to: `;\nCOMMIT;`,
        };
        return replace(options)
    })
    .then(changes => {
        console.log('Modified files:', changes.join(', '));
    })
    .catch(error => {
        console.error('Error occurred:', error);
    });
}

async function main () {
    await deleteAllFinalFiles();
    await convertArgFilesToCsv();
    await parseCsvAndGenerateFinalFileArg();
    await parseCsvAndGenerateFinalFileUsa();
    await opsSqlCodeGenerator();
    await accountsSqlCodeGenerator();
    await finishQueryBuilder();
}

main();