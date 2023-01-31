GET_INV ='''
    query getPartInv($filters: PartInventoriesInputFilters) {
      partInventories(filters: $filters) {
        edges {
          node {
            id
            partId
            quantity
            quantityAvailable
            }
          }
        }
      }
'''

UPDATE_ABOM_MUTATION = '''
    mutation updateAbom($input: UpdateABomItemInput!){
      updateAbomItem(input:$input){
        abomItem{
          id
        }
      }
    }
'''

UPDATE_RUN_STEP_STATUS = '''
    mutation updateRunStepStatus($input: UpdateRunStepInput!){
      updateRunStep(input:$input){
        runStep{
          status
        }
      }
    }
'''