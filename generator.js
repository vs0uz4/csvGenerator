const fs = require('fs')
const { fakerPT_BR: faker } = require('@faker-js/faker')
const fakerBR = require('faker-br')
const cluster = require('cluster')
const numCPUs = require('os').cpus().length

const N = 550000
const outFileName = 'users.csv'

const header = () => {
    return [
        'id',
        'user_name',
        'document',
        'name',
        'mobile_phone',
        'email',
        'address',
        'state',
        'zip_code',
        'latitude',
        'longitude',
        'internet_address',
        'mac_address',
    ]
}
const record = () => {
    let firstName = faker.person.firstName()
    let lastName  = faker.person.lastName()
    let fullName  = `${firstName} ${lastName}`
    let document  = fakerBR.br.cpf()
    let userName  = faker.internet.displayName({ firstName: firstName, lastName: lastName })
    let email     = faker.internet.email({ firstName: firstName, lastName: lastName, provider: 'example.fakerjs.dev', allowSpecialCharacters: false } )
    let state     = faker.location.state( { abbreviated: true } )
    return [
        faker.string.uuid(),
        userName.toLowerCase(),
        document,
        fullName,
        faker.phone.number('(##) 9###-####'),
        email.toLowerCase(),
        faker.location.streetAddress( { useFullAddress: true } ),
        state,
        faker.location.zipCode('#####-###'),
        faker.location.latitude(),
        faker.location.longitude(),
        faker.internet.ipv4(),
        faker.internet.mac( { separator: ':'} )
    ]
}

let workers = []
let written = 0
if (cluster.isMaster) {
    masterProcess()
} else {
    childProcess()
}

function masterProcess () {
    const writer = fs.createWriteStream(outFileName, { flags: 'a' })
    writer.write(header().join(',') + '\n')

    console.log(`Master ${process.pid} is running`)

    for (let i = 0; i < numCPUs; i++) {
        console.log(`Forking process number ${i}...`)
        const worker = cluster.fork()
        worker.on('message', msg => {
            if (msg === 'done') {
                written += 1
            }
            if (written % 100000 === 0) {
                console.log(`written=${written}`)
            }
            if (written >= N) {
                console.log('all done')
                process.exit()
            }
        })
        workers.push(worker)
    }
    for (let i = 0; i < N; i++) {
        const workerId = i % numCPUs
        const worker = workers[workerId]
        worker.send(i)
    }
}

function childProcess () {
    const writer = fs.createWriteStream(outFileName, { flags: 'a' })
    console.log(`Worker ${process.pid} started`)
    process.on('message', () => {
        const msg = record().join(',') + '\n'
        writer.write(msg, () => {
            process.send('done')
        })
    })
}