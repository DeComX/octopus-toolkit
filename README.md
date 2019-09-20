### Prep

### Dev mode:

mongo: `docker-compose -f stack.dev.yml up`

server: `nodemon server.js`

client: `npm start`

### To deploy:

###### Prep

1. Update the client sercret in keys
2. Ensure the API url in client and url service is `https://api.abcer.world/`
3. Ensure the mongo url in keys mongo:27017
4. Ensure the callback in keys "https://api.abcer.world/auth/google/callback"

###### Run
`docker-compose -f stack.yml up -d`
