import express from 'express';
import { isRateLimited } from './rate_limiter.js';

const app = express();
app.use(express.json());

const rateLimiterMiddleWare = async (req,res,next) => {
    try {
        const identifier = req.user?.id || req.ip || req.payment_id || 'user-id';
        const result = await isRateLimited(identifier,5,10)

        res.setHeader('X-RateLimit-Limit', result.limit);
        res.setHeader('X-RateLimit-Remaining', result.remaining);

        if(result.limited) {
            return res.status(429).json({
                success : false,
                message : 'Too many requests. Please try again later'
            })
        }
        next();
    } catch (error) {
        next(error);
    }
}

app.get('/api', rateLimiterMiddleWare, (req,res) => {
    res.json({
        message : 'Welcome to my api'
    })
})

// app.post('/api/payment', rateLimiterMiddleWare, (req , res) => {
//     res.json({
//         success : true,
//         message : 'Hello there'
//     });
// })

app.listen(8080, () => {
    console.log('Server listening on the port 8080')
});