--18 minutos
with tab_origem as (
select
t.id_cpf_cnpj,
t.nu_guia_parcela,
t.da_vencimento,
t.da_pagamento,
t.id_receita,
t.va_principal_original,
t.id_situacao
from bi.fato_lanc_arrec t

--where t.id_cpf_cnpj = '02839741000358'
--where t.nu_guia_parcela IN ( '2006160060420100' )
where t.id_receita like '1%'
and t.id_receita not like '18%'
and t.id_receita not like '17%'
and t.id_situacao in ('00','01','02','03','05','07','08','10','11','18','19','69','78','80') -- tentativa de tirar os valores negativos
and t.nu_guia is not null
),

tab_redir as (

    select distinct h.nu_guia_parcela_origem guia_origem_redir,
    --s.va_principal_original va_principal_original_redir,
    --r.va_principal_original  as va_residuo,
    --sum(nvl(s.va_principal_original,0)) over (order by h.nu_guia_redir, h.nu_guia_parcela_origem ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as acumulado,
    
    --round(r.va_principal_original/(sum(s.va_principal_original) over (partition by h.nu_guia_redir, h.nu_guia_parcela_origem)),5) razao,
    
    sum(case when s.it_co_situacao_lancamento in ('00','01','02','03','05','07','08','10','11','18','19','69','78','80') then s.it_va_principal_original end) over (partition by h.nu_guia_redir, h.nu_guia_parcela_origem) sum_redir,
    --sum(s.va_principal_original) over (partition by h.nu_guia_redir, h.nu_guia_parcela_origem) - nvl(sum(case when s.id_situacao in ('00','02','03') then s.va_principal end) over (partition by h.nu_guia_redir, h.nu_guia_parcela_origem),0) resid_guia_redir,
    nvl(sum(case when s.it_co_situacao_lancamento in ('00','02','03') then s.it_va_principal_original end) over (partition by h.nu_guia_redir, h.nu_guia_parcela_origem),0) va_pago, 
   
   NVL(((sum(case when s.it_co_situacao_lancamento in ('00','02','03') then s.it_va_principal_original end) over (partition by h.nu_guia_redir, h.nu_guia_parcela_origem))) /
   NULLIF((sum(case when s.it_co_situacao_lancamento in ('00','01','02','03','05','07','08','10','11','18','19','69','78','80') then s.it_va_principal_original end) over (partition by h.nu_guia_redir, h.nu_guia_parcela_origem)),0),0) as percentual_pagto,
   
   
   --sum(s.va_principal_original) over (partition by h.nu_guia_parcela_origem order by h.in_nivel_redir ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),

    --round(r.va_principal_original/(sum(r.va_principal_original) over (partition by h.nu_guia_redir)),2) razao,
    count (distinct case when s.it_co_situacao_lancamento in ('00','01','02','03','05','07','08','10','11','18','19','69','78','80') then s.it_nu_guia_lancamento||s.it_nu_parcela end) over (partition by s.it_nu_guia_lancamento) nu_parcelas, -- adicionado depois
    count ( distinct case when s.it_co_situacao_lancamento in ('00','02','03') then s.it_nu_guia_lancamento||s.it_nu_parcela end) over (partition by s.it_nu_guia_lancamento) nu_parcelas_pagas, -- adicionado depois
    h.nu_guia_redir, h.in_nivel_redir,
    count(distinct h.nu_guia_parcela_origem) over (partition by h.nu_guia_redir) qtde_guias_origem,
    count(distinct case when s.it_co_situacao_lancamento = '05' then h.nu_guia_redir end) over (partition by h.nu_guia_parcela_origem) qtde_parcelamentos,
    
    
    case when count (distinct case when s.it_co_situacao_lancamento in ('00','01','02','03','05','07','08','10','11','18','19','69','78','80') then s.it_nu_guia_lancamento||s.it_nu_parcela end) over (partition by s.it_nu_guia_lancamento) 
    = count ( distinct case when s.it_co_situacao_lancamento in ('00','02','03') then s.it_nu_guia_lancamento||s.it_nu_parcela end) over (partition by s.it_nu_guia_lancamento)
    then 'SIM' else 'NAO' end as PARCELAMENTO_QUITADO,
    

    case when count (distinct case when s.it_co_situacao_lancamento in ('00','01','02','03','05','07','08','10','11','18','19','69','78','80') then s.it_nu_guia_lancamento||s.it_nu_parcela end) over (partition by s.it_nu_guia_lancamento) 
    = count ( distinct case when s.it_co_situacao_lancamento in ('00','02','03') then s.it_nu_guia_lancamento||s.it_nu_parcela end) over (partition by s.it_nu_guia_lancamento)
    --a parte acima testa a igualdade = se a quantidade de guias parcelas na situação 00,02,03 é igual a quantidade de guias parcelas totais do redirecionamento
    --se for, considera-se que o parcelamento foi quitado
    then max(to_date(case when s.it_da_pagamento_efetuado = '        ' then null else s.it_da_pagamento_efetuado end,'yyyymmdd') ) 
    keep (dense_rank last order by to_date(case when s.it_da_pagamento_efetuado = '        ' then null else s.it_da_pagamento_efetuado end,'yyyymmdd')) over (partition by s.it_nu_guia_lancamento) end as da_quitacao
    
    from BI.dm_hist_redir_lanc h 
    left join bi.fato_lanc_arrec r on r.nu_guia_parcela = h.nu_guia_parcela_origem -- ORIGEM faz referência aos dados da origem
    left join sitafe.sitafe_lancamento s on s.it_nu_guia_lancamento = h.nu_guia_redir -- REDIR  -- faz referência aos dados do redirecionamento
    where h.nu_guia_parcela_origem in (select f.nu_guia_parcela from tab_origem f)
    and s.it_co_situacao_lancamento in ('00','01','02','03','05','07','08','10','11','18','19','69','78','80') -- codigos válidos
     

),

--select * from tab_redir where tab_redir.guia_origem_redir = '2010010013298200'
--order by 6

tab_concat as (

select distinct

x.id_cpf_cnpj,
x.nu_guia_parcela,
x.id_receita,
x.id_situacao,
y.nu_guia_redir,
y.in_nivel_redir,
x.va_principal_original, 
--y.va_principal_original_redir,
--y.resid_guia_redir,
NVL(y.va_pago,0) va_pago,
--round(y.va_pago*(x.va_principal_original/max(y.sum_redir) over (partition by x.nu_guia_parcela)),5) va_pago_apropriado,
NVL(round(y.percentual_pagto,5),0) as percentual_pagto,

/*
round(exp(sum(ln(case when y.percentual_pagto <> 0 then y.percentual_pagto end))
    over (partition by x.nu_guia_parcela order by y.in_nivel_redir ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )),5) as acum_pago,
*/ 
case when 1-y.percentual_pagto = 0 then 0 else (

round(exp(sum(ln(case when 1-y.percentual_pagto > 0 then 1-y.percentual_pagto end))
    over (partition by x.nu_guia_parcela order by y.in_nivel_redir ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )),5) 
    
        ) end as acum_resid,
        
        

round(exp(sum(ln(case when 1-y.percentual_pagto > 0 then 1-y.percentual_pagto end))
    over (partition by x.nu_guia_parcela order by y.in_nivel_redir ROWS UNBOUNDED PRECEDING )),5) as acum_resid_nivel_anterior,


--y.acumulado,
--x.va_principal_original - round(y.va_pago*(x.va_principal_original),2) va_original_ajust,
y.sum_redir,
(nvl(y.sum_redir,0) - nvl(y.va_pago,0)) as residuo,
--y.razao,
--case when y.in_nivel_redir = 1 then (x.va_principal_original/sum_redir) end as nivel_1,--max(y.sum_redir) over (partition by x.nu_guia_parcela) razao,
y.nu_parcelas,
y.nu_parcelas_pagas,
y.parcelamento_quitado,
x.da_vencimento,


    
case when y.in_nivel_redir is null then 
x.da_pagamento 
else 
max(y.da_quitacao) keep (dense_rank first order by y.da_quitacao) over (partition by x.nu_guia_parcela)
end as da_quitacao,


y.qtde_guias_origem, --//over (partition by ),
y.qtde_parcelamentos
/*
case when y.nu_guia_redir is null and x.id_situacao in ('00','02','03') then x.va_principal_original  
     when y.nu_guia_redir is not null then 
     (
     select nvl(sum(arrec.va_principal_original),0)--* y.razao
     from bi.fato_lanc_arrec arrec
     where arrec.nu_guia = y.nu_guia_redir and arrec.id_situacao in ('00','02','03')
     
        )
     
     
     end as valor_pago
     
    */


from tab_origem x
left join tab_redir y on x.nu_guia_parcela = y.guia_origem_redir
order by 3

),

tab_result as (

    select  t.*,
    
    case when t.in_nivel_redir is null then 'NAO' else 'SIM' end as REDIRECIONADO,
    
    NVL(case when t.in_nivel_redir = 1 then (t.va_principal_original/NULLIF(t.sum_redir,0) )
    else 
    case when t.residuo = 0 then 1 else (t.va_principal_original*t.acum_resid)/NULLIF(residuo,0)
    end end,0) as razao_resid_mult,
    
    NVL(    
    case when t.in_nivel_redir is null then (case when t.id_situacao in ('00','02','03') then t.va_principal_original end)
    else
    case when t.residuo = 0 and t.in_nivel_redir <> 1 then t.va_principal_original * t.acum_resid_nivel_anterior
    else (t.va_pago    
    * 
    case when t.in_nivel_redir = 1 then (t.va_principal_original/NULLIF(t.sum_redir,0) )
    else ( t.va_principal_original*t.acum_resid)/NULLIF(residuo,0)
    end) end end, 0) as va_pago_apropriado
    
    
    from tab_concat t 

    )


    select 
    
    k.id_cpf_cnpj,
    k.nu_guia_parcela,
    k.id_receita,
    k.va_principal_original,
    round(sum(k.va_pago_apropriado),2) va_pago_apropriado,
    round((k.va_principal_original-sum(k.va_pago_apropriado))/ NULLIF(k.va_principal_original,0),4) as inad,
    k.redirecionado,
    k.qtde_parcelamentos,
    --k.qtde_guias_origem,
    k.da_vencimento,
    k.da_quitacao
    
    from tab_result k --WHERE K.NU_GUIA_PARCELA = '2021110045412400'
    group by
    k.va_principal_original,
    k.redirecionado,
    k.qtde_parcelamentos,
    --k.qtde_guias_origem,
    k.nu_guia_parcela,
    k.id_receita,
    k.id_cpf_cnpj,
    k.da_vencimento,
    k.da_quitacao
    --order by 3
    

-- se atentar que algumas dívidas só entram no parcelamento depois (por exemplo, quando
-- já há um re(parcelamento) em curso... NÃO TRATEI ESSA PARTE. RETIFICANDO: TRATADO 03/04/2022
-- NESSE exemplo aqui, o valor da soma de pagos ficou menor que o valor original da dívida. Porque?
--RESOLVIDO EM 03/04/2022. EU ESTAVA UTILIZANDO VA_PRINCIPAL AO INVÉS DE VA_PRINCIPAL_ORIGINAL
-- uma IDEIA: adicionar a coluna "número de parcelas" ao lado da coluna "nu_guia_redir". 
----- após adicionar a coluna "número de parcelas pagas" assim teríamos outro meio
----- indireto de aferir se foi paga toda a dívida.FEITO EM 02/04/2022
--- PROXIMA TAREFA: ACREDITO QUE ALGUNS BUGS AINDA PODEM ACONTECER. SERÁ NECESSÁRIO FAZER NOVOS TESTES.


