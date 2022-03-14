
select p.co_regime_pagto, d.no_regime_pagamento ,count(p.co_cnpj_cpf) total
from bi.dm_pessoa p
left join BI.dm_regime_pagto_descricao d on p.co_regime_pagto = d.co_regime_pagamento
where p.in_situacao = '001' -- somente contribuintes ativos
group by p.co_regime_pagto, d.no_regime_pagamento
order by 1