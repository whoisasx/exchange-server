use crate::pool::pool;

pub async fn link_fill_to_position(position_id: i64, fill_id: i64) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO position_fills(position_id, fill_id)
        VALUES($1,$2)
        "#,
        position_id,
        fill_id
    )
    .execute(pool())
    .await?;

    Ok(())
}

pub async fn get_fill_ids_by_position_id(position_id: i64) -> Result<Vec<i64>, sqlx::Error> {
    let fill_ids = sqlx::query_scalar!(
        r#"
        SELECT fill_id
        FROM position_fills
        WHERE position_id=$1
        "#,
        position_id
    )
    .fetch_all(pool())
    .await?;

    Ok(fill_ids)
}

pub async fn get_position_ids_by_fill_id(fill_id: i64) -> Result<Vec<i64>, sqlx::Error> {
    let position_ids = sqlx::query_scalar!(
        r#"
        SELECT position_id
        FROM position_fills
        WHERE fill_id=$1
        "#,
        fill_id
    )
    .fetch_all(pool())
    .await?;

    Ok(position_ids)
}
