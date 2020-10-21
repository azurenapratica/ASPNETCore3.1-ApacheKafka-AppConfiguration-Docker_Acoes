using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;
using APIAcoes.Models;

namespace APIAcoes.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class AcoesController : ControllerBase
    {
        [HttpPost]
        [ProducesResponseType(typeof(Resultado), (int)HttpStatusCode.OK)]
        [ProducesResponseType((int)HttpStatusCode.BadRequest)]
        public async Task<Resultado> Post(
            [FromServices] IConfiguration configuration,
            [FromServices]ILogger<AcoesController> logger,
            Acao acao)
        {
            var cotacaoAcao = new CotacaoAcao()
            {
                Codigo = acao.Codigo,
                Valor = acao.Valor,
                CodCorretora = configuration["Corretora:Codigo"],
                NomeCorretora = configuration["Corretora:Nome"]
            };
            var conteudoAcao = JsonSerializer.Serialize(cotacaoAcao);
            logger.LogInformation($"Dados: {conteudoAcao}");

            string topic = configuration["ApacheKafka:Topic"];
            var configKafka = new ProducerConfig
            {
                BootstrapServers = configuration["ApacheKafka:Broker"]
            };

            using (var producer = new ProducerBuilder<Null, string>(configKafka).Build())
            {
                var result = await producer.ProduceAsync(
                    topic,
                    new Message<Null, string>
                    { Value = conteudoAcao });

                logger.LogInformation(
                    $"Apache Kafka - Envio para o tópico {topic} concluído | " +
                    $"{conteudoAcao} | Status: { result.Status.ToString()}");
            }

            return new Resultado()
            {
                Mensagem = "Informações de ação enviadas com sucesso!"
            };
        }
    }
}